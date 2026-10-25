#!/usr/bin/env node
/* ============================================================
   飞秋工作室 · 本地代理服务
   ------------------------------------------------------------
   1) AI 转发代理  —— 解决阿里云百炼等中转站无 CORS 头的问题
   2) 文件上传下载 —— 让群聊能真正传文件与图片
   3) WebSocket 群聊 —— 多端实时互通
   4) 静态托管     —— 顺带把工作台也托管了

   启动：node feiq-proxy.js [--port 8787]
   ============================================================ */
"use strict";

const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const url = require("url");
const crypto = require("crypto");

/* ---------- 可选依赖：ws（缺失时群聊降级为 HTTP 轮询） ---------- */
let WebSocketServer = null;
try { WebSocketServer = require("ws").WebSocketServer; } catch (e) { /* 可选 */ }

/* ---------- 参数 ---------- */
const argPort = (() => {
  const i = process.argv.indexOf("--port");
  return i > 0 ? Number(process.argv[i + 1]) : 8787;
})();
const PORT = Number(process.env.FEIQ_PROXY_PORT) || argPort;
const HTML_FILE = path.join(__dirname, "index.html");
const UPLOAD_DIR = path.join(__dirname, ".feiq-uploads");
const MAX_FILE = 100 * 1024 * 1024;   // 100 MB

/* 本节点身份：局域网里别人看到的我 */
const NODE_NAME = process.env.FEIQ_NODE_NAME || require("os").hostname();
const NODE_LABEL = process.env.FEIQ_NODE_LABEL || "飞秋工作站";

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------- 内存态 ---------- */
const rooms = new Map();      // roomId -> { msgs: [], members: Set }
const files = new Map();      // fileId -> { name, size, mime, path, sender, time }
const MAX_MSG = 500;
let msgSeq = 0;

/* ---------- 工具 ---------- */
function readBody(req, limit = MAX_FILE) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > limit) { reject(new Error("请求体过大（上限 " + Math.round(limit / 1048576) + " MB）")); req.destroy(); return; }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}
function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "cache-control": "no-store"
  });
  res.end(body);
}
function log(...a) { console.log("[" + new Date().toISOString().slice(11, 19) + "]", ...a); }
function cors(req, res) {
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("access-control-allow-headers", "*");
  res.setHeader("access-control-allow-methods", "GET,POST,OPTIONS,DELETE");
  res.setHeader("access-control-expose-headers", "*");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return true; }
  return false;
}
function humanSize(n) {
  if (!n && n !== 0) return "";
  if (n < 1024) return n + " B";
  if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
  if (n < 1073741824) return (n / 1048576).toFixed(1) + " MB";
  return (n / 1073741824).toFixed(2) + " GB";
}

/* ============================================================
   1) AI 代理
   ============================================================ */
async function handleAiProxy(req, res) {
  let payload;
  try {
    payload = JSON.parse((await readBody(req, 32 * 1024 * 1024)).toString("utf8"));
  } catch (e) {
    return json(res, 400, { error: "请求体不是合法 JSON" });
  }
  const target = payload.baseUrl;
  if (!target) return json(res, 400, { error: "缺少 baseUrl" });
  if (!/^https?:\/\//i.test(target)) return json(res, 400, { error: "baseUrl 必须以 http(s):// 开头" });

  let u;
  try { u = new URL(target); } catch (e) { return json(res, 400, { error: "baseUrl 格式错误" }); }
  const mod = u.protocol === "https:" ? https : http;

  const headers = Object.assign({}, payload.headers || {});
  headers["host"] = u.host;
  if (payload.body !== undefined) headers["content-length"] = Buffer.byteLength(payload.body);
  /* GET 请求不带 body —— content-length/content-type 必须剔除，否则部分服务器（如智谱）直接掐断连接 */
  const m = (payload.method || "POST").toUpperCase();
  if (m === "GET" || m === "HEAD") { delete headers["content-length"]; delete headers["content-type"]; }

  log("→ AI 代理", u.toString(), payload.stream ? "(stream)" : "");

  const upstream = mod.request({
    method: payload.method || "POST",   /* 支持 GET（拉模型列表 /models） */
    hostname: u.hostname,
    port: u.port || (u.protocol === "https:" ? 443 : 80),
    path: u.pathname + u.search,
    headers: headers,
    timeout: 300000
  }, (ures) => {
    res.writeHead(ures.statusCode || 502, {
      "content-type": ures.headers["content-type"] || "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      "connection": "keep-alive",
      "access-control-allow-origin": "*",
      "x-accel-buffering": "no"
    });
    ures.pipe(res);
    ures.on("end", () => res.end());
  });

  upstream.on("error", (e) => {
    log("✗ 上游错误", e.message);
    if (!res.headersSent) json(res, 502, { error: "无法连接目标 API：" + e.message });
    else res.end();
  });
  upstream.on("timeout", () => { upstream.destroy(); if (!res.headersSent) json(res, 504, { error: "上游超时" }); });

  if (payload.body !== undefined) upstream.write(payload.body);
  upstream.end();
}

/* ============================================================
   2) 文件上传 / 下载
   ------------------------------------------------------------
   上传：POST /api/file/upload
     ?name=<urlencoded 文件名>&mime=<类型>&sender=<发送者>
     body = 原始二进制
   下载：GET /api/file/<fileId>
   ============================================================ */
async function handleUpload(req, res, q) {
  const name = decodeURIComponent(String(q.name || "file.bin")).replace(/[\\/:*?"<>|]/g, "_");
  const mime = String(q.mime || "application/octet-stream");
  const sender = String(q.sender || "匿名");
  let buf;
  try {
    buf = await readBody(req);
  } catch (e) {
    return json(res, 413, { error: e.message });
  }
  if (!buf.length) return json(res, 400, { error: "文件内容为空" });

  const id = crypto.randomBytes(16).toString("hex");
  const ext = path.extname(name) || "";
  const disk = path.join(UPLOAD_DIR, id + ext);
  fs.writeFileSync(disk, buf);
  const meta = {
    id: id, name: name, size: buf.length, mime: mime,
    sender: sender, time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" }),
    url: "/api/file/" + id
  };
  files.set(id, Object.assign({}, meta, { disk: disk }));
  log("↑ 上传", name, humanSize(buf.length), "by", sender);
  json(res, 200, { ok: true, file: { id: meta.id, name: meta.name, size: meta.size, mime: meta.mime, url: meta.url, sender: meta.sender, time: meta.time } });
}

function handleDownload(req, res, fileId) {
  const meta = files.get(fileId);
  if (!meta || !fs.existsSync(meta.disk)) {
    return json(res, 404, { error: "文件不存在或已被清理" });
  }
  const stat = fs.statSync(meta.disk);
  // 中文文件名需要 RFC 5987 编码，否则 Node 会写 latin1 字节导致响应头损坏
  const asciiName = meta.name.replace(/[^\x20-\x7e]/g, "_").replace(/"/g, "");
  const disp = 'inline; filename="' + asciiName + '"; filename*=UTF-8\'\'' + encodeURIComponent(meta.name);
  res.writeHead(200, {
    "content-type": meta.mime || "application/octet-stream",
    "content-length": stat.size,
    "content-disposition": disp,
    "access-control-allow-origin": "*",
    "cache-control": "no-store"
  });
  fs.createReadStream(meta.disk).pipe(res);
}

function handleFileInfo(req, res, fileId) {
  const meta = files.get(fileId);
  if (!meta) return json(res, 404, { error: "文件不存在" });
  json(res, 200, { ok: true, file: { id: meta.id, name: meta.name, size: meta.size, mime: meta.mime, url: meta.url, sender: meta.sender, time: meta.time } });
}

function handleFileList(req, res) {
  const list = Array.from(files.values()).map(function (m) {
    return { id: m.id, name: m.name, size: m.size, mime: m.mime, url: m.url, sender: m.sender, time: m.time };
  });
  json(res, 200, { ok: true, files: list, count: list.length, totalSize: list.reduce((s, m) => s + m.size, 0) });
}

/* ============================================================
   3) 群聊消息
   ============================================================ */
async function handleChatSend(req, res) {
  let payload;
  try {
    payload = JSON.parse((await readBody(req, 4 * 1024 * 1024)).toString("utf8"));
  } catch (e) {
    return json(res, 400, { error: "请求体错误" });
  }
  const room = String(payload.room || "default");
  const msg = {
    id: "m" + (++msgSeq),
    room: room,
    from: String(payload.from || "匿名"),
    type: payload.type || "text",
    text: String(payload.text != null ? payload.text : (payload.t != null ? payload.t : "")),
    t: String(payload.text != null ? payload.text : (payload.t != null ? payload.t : "")),
    _cid: payload.cid || "",
    file: payload.file || null,
    size: payload.size || null,
    icon: payload.icon || null,
    mime: payload.mime || null,
    url: payload.url || null,
    fileId: payload.fileId || null,
    bytes: payload.bytes || null,
    time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })
  };
  if (!rooms.has(room)) rooms.set(room, { msgs: [], members: new Set() });
  const r = rooms.get(room);
  r.msgs.push(msg);
  if (r.msgs.length > MAX_MSG) r.msgs.splice(0, r.msgs.length - MAX_MSG);
  log("→ 群聊", room, msg.from + ":", msg.text || (msg.file && msg.file.name) || "");
  broadcast(room, { type: "message", msg: msg });
  json(res, 200, { ok: true, id: msg.id, online: r.members.size });
}

function handleChatHistory(req, res, q) {
  const room = String(q.room || "default");
  const r = rooms.get(room) || { msgs: [] };
  json(res, 200, { ok: true, room: room, msgs: r.msgs, online: r.members.size });
}

/* ============================================================
   4) WebSocket
   ============================================================ */
const sockets = new Set();

function broadcast(room, data) {
  const raw = JSON.stringify(data);
  sockets.forEach(function (ws) {
    try { if (ws.room === room) ws.send(raw); } catch (e) { /* ignore */ }
  });
}

function setupWs(server) {
  if (!WebSocketServer) {
    log("! 未安装 ws 包，WebSocket 不可用（群聊将退回 HTTP 模式）");
    return;
  }
  const wss = new WebSocketServer({ server: server, path: "/ws" });
  wss.on("connection", function (ws, req) {
    const q = url.parse(req.url, true).query;
    ws.room = String(q.room || "default");
    ws.user = String(q.user || "匿名");
    sockets.add(ws);

    if (!rooms.has(ws.room)) rooms.set(ws.room, { msgs: [], members: new Set() });
    const r = rooms.get(ws.room);
    r.members.add(ws.user);

    log("⇅ WS 连接", ws.room, ws.user, "(" + r.members.size + " 人在线)");
    ws.send(JSON.stringify({ type: "history", room: ws.room, msgs: r.msgs, online: r.members.size }));
    broadcast(ws.room, { type: "online", online: r.members.size });

    ws.on("message", function (buf) {
      try {
        const m = JSON.parse(buf.toString());
        if (m.type === "chat") {
          const msg = {
            id: "m" + (++msgSeq), room: ws.room, from: ws.user,
            type: m.msgType || "text",
            text: String(m.text != null ? m.text : (m.t != null ? m.t : "")),
            t: String(m.text != null ? m.text : (m.t != null ? m.t : "")),
            _cid: m.cid || "",
            file: m.file || null, size: m.size || null, icon: m.icon || null, mime: m.mime || null,
            url: m.url || null, fileId: m.fileId || null, bytes: m.bytes || null,
            time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })
          };
          r.msgs.push(msg);
          if (r.msgs.length > MAX_MSG) r.msgs.splice(0, r.msgs.length - MAX_MSG);
          broadcast(ws.room, { type: "message", msg: msg });
        }
      } catch (e) { /* ignore */ }
    });

    ws.on("close", function () {
      sockets.delete(ws);
      r.members.delete(ws.user);
      broadcast(ws.room, { type: "online", online: r.members.size });
      log("⇅ WS 断开", ws.room, ws.user);
    });
  });
}

/* ============================================================
   4.5) UDP：IP 直发（对标 FeiQ 的 IPMsg / SendARP 通道）
   ------------------------------------------------------------
   监听 2425 端口（FeiQ 默认 IPMsg 端口）：
   - 收到 UDP 包 → 推给所有前端（前端显示"收到 IP 消息"）
   - 前端 POST /api/ip/send → 代理用 UDP 真实发到目标 IP
   ============================================================ */
const dgram = require("dgram");
const UDP_PORT = Number(process.env.FEIQ_UDP_PORT) || 2425;
let udpSock = null;
let UDP_ACTUAL_PORT = UDP_PORT;
const udpLog = [];

function setupUdp() {
  // bind 的失败是异步 error 事件，不能用 try/catch 接。
  // 依次尝试：0.0.0.0:2425 → 127.0.0.1:2425 → 127.0.0.1:0(随机)
  const tries = [["0.0.0.0", UDP_PORT], ["127.0.0.1", UDP_PORT], ["127.0.0.1", 0]];
  let ti = 0;

  function nextTry() {
    if (ti >= tries.length) {
      log("! UDP 端口绑定全部失败，IP 直发接收不可用（发送仍可用）");
      udpSock = null;
      return;
    }
    const host = tries[ti][0], port = tries[ti][1];
    ti++;
    const sock = dgram.createSocket({ type: "udp4", reuseAddr: true });
    let settled = false;

    sock.on("error", function (err) {
      if (settled) return;
      settled = true;
      try { sock.close(); } catch (e) {}
      log("! UDP 绑定", host + ":" + port, "失败：", err.message);
      nextTry();
    });

    sock.on("listening", function () {
      if (settled) return;
      settled = true;
      udpSock = sock;
      UDP_ACTUAL_PORT = sock.address().port;
      log("[UDP] 已监听 " + UDP_ACTUAL_PORT + "（IP 直发接收）");

      sock.on("message", function (buf, rinfo) {
        const text = buf.toString("utf8").slice(0, 2000);
        const item = {
          from: rinfo.address, port: rinfo.port, text: text,
          time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })
        };
        udpLog.push(item);
        if (udpLog.length > 100) udpLog.splice(0, udpLog.length - 100);
        log("[UDP] 收到", rinfo.address + ":" + rinfo.port, text.slice(0, 60));
        const raw = JSON.stringify({ type: "udp", item: item });
        sockets.forEach(function (ws) { try { ws.send(raw); } catch (e) {} });
      });
    });

    sock.bind(port, host);
  }

  nextTry();
}

async function handleIpSend(req, res) {
  let payload;
  try {
    payload = JSON.parse((await readBody(req, 1024 * 1024)).toString("utf8"));
  } catch (e) {
    return json(res, 400, { error: "请求体错误" });
  }
  const target = String(payload.ip || "").trim();
  const port = Number(payload.port) || UDP_ACTUAL_PORT;
  const text = String(payload.text || "");
  if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(target)) {
    return json(res, 400, { error: "IP 地址格式不正确：" + target });
  }
  const buf = Buffer.from(text, "utf8");
  const sock = dgram.createSocket("udp4");
  sock.send(buf, 0, buf.length, port, target, function (err) {
    sock.close();
    if (err) {
      log("[UDP] 发送失败", target + ":" + port, err.message);
      return json(res, 502, { error: "UDP 发送失败：" + err.message });
    }
    log("[UDP] 已发送 ->", target + ":" + port, text.slice(0, 60));
    json(res, 200, { ok: true, to: target, port: port, bytes: buf.length });
  });
}

function handleIpLog(req, res) {
  json(res, 200, { ok: true, items: udpLog, port: UDP_ACTUAL_PORT, listening: !!udpSock });
}

/* ============================================================
   4.6) 节点注册与局域网发现
   ------------------------------------------------------------
   - 每台机器启动时把「自己」注册到本机节点表
   - 节点信息：名称、IP、端口、在线时间
   - 提供 /api/nodes（列出） / /api/nodes/ping（探测某 IP 是否可达）
   - UDP 广播信标：定时向 255.255.255.255 广播自己的存在
     其它机器收到后即可把自己也加进对方的节点表（互发现）
   ============================================================ */
const nodes = new Map();   // ip -> { ip, name, label, port, since, lastSeen }

/* 注册自己 */
function registerSelf() {
  const ips = localIPv4s();
  const addrs = ips.length ? ips.map(function (i) { return i.addr; }) : ["127.0.0.1"];
  addrs.forEach(function (ip) {
    nodes.set(ip, {
      ip: ip, name: NODE_NAME, label: NODE_LABEL, port: PORT,
      since: Date.now(), lastSeen: Date.now(), self: true
    });
  });
}

/* 广播信标（UDP 2426，与 IP 直发的 2425 分开） */
const BEACON_PORT = Number(process.env.FEIQ_BEACON_PORT) || 2426;
let beaconSock = null;
function setupBeacon() {
  try {
    beaconSock = dgram.createSocket({ type: "udp4", reuseAddr: true });
    beaconSock.on("error", function (e) {
      log("! 信标错误：", e.message);
      try { beaconSock.close(); } catch (x) {}
      beaconSock = null;
    });
    beaconSock.on("message", function (buf, rinfo) {
      let info;
      try { info = JSON.parse(buf.toString("utf8")); } catch (e) { return; }
      if (!info || !info.name) return;
      // 收到别人的信标 → 记录为已知节点
      upsertNode({
        ip: rinfo.address, name: info.name, label: info.label || "飞秋工作站",
        port: info.port || PORT
      });
      log("[发现] " + rinfo.address + " · " + info.name);
    });
    beaconSock.bind(BEACON_PORT, "0.0.0.0", function () {
      try { beaconSock.setBroadcast(true); } catch (e) {}
      setInterval(function () {
        if (!beaconSock) return;
        const payload = Buffer.from(JSON.stringify({
          name: NODE_NAME, label: NODE_LABEL, port: PORT, ts: Date.now()
        }));
        // 广播到全局 + 各网段定向广播
        const targets = ["255.255.255.255"];
        localIPv4s().forEach(function (i) {
          const p = i.addr.split(".");
          targets.push(p[0] + "." + p[1] + "." + p[2] + ".255");
        });
        targets.forEach(function (t) {
          try { beaconSock.send(payload, 0, payload.length, BEACON_PORT, t, function () {}); } catch (e) {}
        });
      }, 5000);
      log("[信标] 已启动，广播端口 " + BEACON_PORT + "（每 5 秒一次）");
    });
  } catch (e) {
    log("! 信标初始化失败：", e.message);
  }
}
function upsertNode(n) {
  const old = nodes.get(n.ip);
  nodes.set(n.ip, Object.assign({}, old || {}, n, { lastSeen: Date.now() }));
  // 广播新节点给前端
  const raw = JSON.stringify({ type: "node", node: nodes.get(n.ip) });
  sockets.forEach(function (ws) { try { ws.send(raw); } catch (e) {} });
}
function handleNodesList(req, res) {
  // 清理 60 秒未见过的节点
  const now = Date.now();
  Array.from(nodes.keys()).forEach(function (ip) {
    const n = nodes.get(ip);
    if (!n.self && now - n.lastSeen > 60000) nodes.delete(ip);
  });
  const list = Array.from(nodes.values()).map(function (n) {
    return { ip: n.ip, name: n.name, label: n.label, port: n.port, self: !!n.self, since: n.since };
  });
  json(res, 200, { ok: true, nodes: list, self: { name: NODE_NAME, label: NODE_LABEL, port: PORT } });
}
/* 主动 ping 某个 IP：判断对方是否在跑本代理 */
function handleNodesPing(req, res, q) {
  const ip = String(q.ip || "").trim();
  if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) {
    return json(res, 400, { error: "IP 格式不正确" });
  }
  const port = Number(q.port) || PORT;
  const sock = dgram.createSocket("udp4");
  const payload = Buffer.from(JSON.stringify({
    name: NODE_NAME, label: NODE_LABEL, port: PORT, ts: Date.now(), probe: true
  }));
  let done = false;
  const finish = function (alive) {
    if (done) return;
    done = true;
    try { sock.close(); } catch (e) {}
    json(res, 200, { ok: true, ip: ip, alive: alive, port: port });
  };
  sock.on("message", function () { finish(true); });
  sock.on("error", function () { finish(false); });
  try {
    sock.send(payload, 0, payload.length, BEACON_PORT, ip, function (err) {
      if (err) finish(false);
    });
  } catch (e) { finish(false); }
  setTimeout(function () { finish(false); }, 1500);
}

/* ============================================================
   5) 静态托管
   ============================================================ */
function serveStatic(req, res, pathname) {
  const f = pathname === "/" ? "/index.html" : pathname;
  const file = path.join(__dirname, decodeURIComponent(f));
  if (!file.startsWith(__dirname) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404); res.end("Not found"); return;
  }
  const type = file.endsWith(".html") ? "text/html; charset=utf-8"
    : file.endsWith(".js") ? "application/javascript; charset=utf-8"
    : file.endsWith(".css") ? "text/css; charset=utf-8"
    : file.endsWith(".json") ? "application/json; charset=utf-8"
    : "application/octet-stream";
  res.writeHead(200, { "content-type": type, "cache-control": "no-store" });
  fs.createReadStream(file).pipe(res);
}

/* ============================================================
   主服务
   ============================================================ */
const server = http.createServer(async function (req, res) {
  if (cors(req, res)) return;
  const parsed = url.parse(req.url, true);
  const p = parsed.pathname;

  try {
    if (p === "/api/ai/chat") return await handleAiProxy(req, res);
    if (p === "/api/file/upload" && req.method === "POST") return await handleUpload(req, res, parsed.query);
    if (p === "/api/file/list") return handleFileList(req, res);
    if (p.startsWith("/api/file/")) {
      const id = p.slice("/api/file/".length);
      if (!id) return json(res, 400, { error: "缺少文件 id" });
      if (req.method === "DELETE") {
        const m = files.get(id);
        if (m) { try { fs.unlinkSync(m.disk); } catch (e) {} files.delete(id); }
        return json(res, 200, { ok: true });
      }
      // GET 默认返回文件内容（下载）；需要元信息用 ?meta=1
      if (parsed.query.meta === "1") return handleFileInfo(req, res, id);
      return handleDownload(req, res, id);
    }
    if (p === "/api/ip/send" && req.method === "POST") return await handleIpSend(req, res);
    if (p === "/api/ip/log") return handleIpLog(req, res);
    if (p === "/api/nodes") return handleNodesList(req, res);
    if (p === "/api/nodes/ping") return handleNodesPing(req, res, parsed.query);
    if (p === "/api/chat/send" && req.method === "POST") return await handleChatSend(req, res);
    if (p === "/api/chat/history") return handleChatHistory(req, res, parsed.query);
    if (p === "/api/chat/clear" && req.method === "POST") {
      const room = String(parsed.query.room || "default");
      const r = rooms.get(room);
      if (r) { r.msgs = []; log("[清空] 房间", room); }
      broadcast(room, { type: "cleared" });
      return json(res, 200, { ok: true, room: room });
    }
    if (p === "/api/health") {
      return json(res, 200, {
        ok: true, service: "feiq-proxy", version: "1.1",
        ws: !!WebSocketServer, rooms: rooms.size, sockets: sockets.size,
        files: files.size, udp: udpSock ? UDP_ACTUAL_PORT : false,
        node: NODE_NAME, nodes: nodes.size, beacon: !!beaconSock,
        uptime: Math.round(process.uptime())
      });
    }
    if (p.startsWith("/api/")) return json(res, 404, { error: "未知接口 " + p });
    return serveStatic(req, res, p);
  } catch (e) {
    log("✗", e && e.message);
    if (!res.headersSent) json(res, 500, { error: String(e && e.message || e) });
    else res.end();
  }
});

setupWs(server);
setupUdp();
registerSelf();
setupBeacon();

/* 列出本机所有 IPv4，供局域网访问 */
function localIPv4s() {
  const os = require("os");
  const out = [];
  // 虚拟网卡：这些段一般是虚拟化/隧道网卡，对局域网通信没意义
  const VIRTUAL = /vmware|virtualbox|vethernet|hyper-v|loopback|docker|wsl|tailscale|zerotier|hamachi|npcap|pseudo|teredo|isatap|6in4|comcast|rsrc|vpn/i;
  const ifs = os.networkInterfaces();
  Object.keys(ifs).forEach(function (name) {
    if (VIRTUAL.test(name)) return;
    (ifs[name] || []).forEach(function (i) {
      if (i.family !== "IPv4" || i.internal) return;
      // 过滤掉明显是虚拟网段的地址（192.168.x 里 x 很小的一般是虚拟网卡，但无法完全判定，保留）
      out.push({ name: name, addr: i.address });
    });
  });
  // 去重
  const seen = {}, uniq = [];
  out.forEach(function (i) { if (!seen[i.addr]) { seen[i.addr] = 1; uniq.push(i); } });
  return uniq;
}

server.listen(PORT, "0.0.0.0", function () {
  console.log("");
  console.log("  飞秋工作室 · 本地代理已启动");
  console.log("  ─────────────────────────────────────────");
  console.log("  工作台     http://127.0.0.1:" + PORT + "/");
  const ips = localIPv4s();
  if (ips.length) {
    console.log("  局域网访问：");
    ips.forEach(function (i) { console.log("             http://" + i.addr + ":" + PORT + "/   (" + i.name + ")"); });
  } else {
    console.log("  局域网访问：未检测到局域网网卡");
  }
  console.log("  节点名称   " + NODE_NAME + "  (可在环境变量 FEIQ_NODE_NAME 改)");
  console.log("  AI 代理    POST /api/ai/chat");
  console.log("  文件上传   POST /api/file/upload?name=xx&mime=xx");
  console.log("  文件下载   GET  /api/file/<id>");
  console.log("  群聊 WS    ws://127.0.0.1:" + PORT + "/ws?room=X&user=Y");
  console.log("  IP 直发    POST /api/ip/send  (UDP 监听 " + UDP_PORT + ")");
  console.log("  节点发现   GET  /api/nodes   (UDP 信标 " + BEACON_PORT + ")");
  console.log("  健康检查   GET  /api/health");
  console.log("");
  console.log("  页面里的「服务商」选「本地代理」即可。");
  console.log("  群聊：多个浏览器标签打开同一地址、同一群名，即可互相看到消息。");
  console.log("  文件上传目录：" + UPLOAD_DIR);
  console.log("");
  console.log("  按 Ctrl+C 停止");
  console.log("");
});

process.on("SIGINT", function () { console.log("\n已停止。"); process.exit(0); });
