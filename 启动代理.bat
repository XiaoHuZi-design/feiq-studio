@echo off
chcp 65001 >nul
title FeiQ Studio - Local Proxy
cd /d "%~dp0"

set "NODE_EXE="
where node >nul 2>nul
if %errorlevel%==0 set "NODE_EXE=node"
if not defined NODE_EXE if exist "E:\APP\nodejs\node.exe" set "NODE_EXE=E:\APP\nodejs\node.exe"
if not defined NODE_EXE if exist "C:\Program Files\nodejs\node.exe" set "NODE_EXE=C:\Program Files\nodejs\node.exe"

if not defined NODE_EXE (
  echo.
  echo [ERROR] Node.js not found.
  echo Install from https://nodejs.org then run this file again.
  echo.
  pause
  exit /b 1
)

echo.
echo Starting local proxy ...  keep this window OPEN.
echo It only forwards requests to the AI vendor. Your API key is NOT saved.
echo.
"%NODE_EXE%" feiq-proxy.js
echo.
echo Proxy stopped.
pause
