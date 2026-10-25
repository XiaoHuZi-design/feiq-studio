' silently start feiq-proxy
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh = CreateObject("WScript.Shell")
dir = fso.GetParentFolderName(WScript.ScriptFullName)
Set exec = sh.Exec("cmd /c netstat -ano | findstr :8787 | findstr LISTENING")
out = ""
Do While exec.Status = 0
  On Error Resume Next
  out = out & exec.StdOut.ReadAll()
Loop
If InStr(out, "LISTENING") = 0 Then
  sh.CurrentDirectory = dir
  sh.Run Chr(34) & "E:\\APP\\nodejs\\node.exe" & Chr(34) & " feiq-proxy.js", 0, False
End If
