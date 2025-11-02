param(
  [int]$Port = 8080
)

$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Web

function Get-ContentType([string]$path){
  switch ([IO.Path]::GetExtension($path).ToLower()){
    '.html' { 'text/html; charset=utf-8' }
    '.css'  { 'text/css; charset=utf-8' }
    '.js'   { 'application/javascript; charset=utf-8' }
    '.json' { 'application/json; charset=utf-8' }
    '.png'  { 'image/png' }
    '.jpg'  { 'image/jpeg' }
    '.svg'  { 'image/svg+xml' }
    default { 'text/plain; charset=utf-8' }
  }
}

function Send-Json($ctx, $obj, [int]$code=200){
  $json = ($obj | ConvertTo-Json -Depth 6)
  $bytes = [Text.Encoding]::UTF8.GetBytes($json)
  $ctx.Response.StatusCode = $code
  $ctx.Response.ContentType = 'application/json; charset=utf-8'
  $ctx.Response.AddHeader('Access-Control-Allow-Origin','*')
  $ctx.Response.OutputStream.Write($bytes,0,$bytes.Length)
  $ctx.Response.Close()
}

function Send-File($ctx, [string]$fsPath){
  if (-not (Test-Path $fsPath)) { Send-Json $ctx @{ ok=$false; error='Not Found' } 404; return }
  $bytes = [IO.File]::ReadAllBytes($fsPath)
  $ctx.Response.StatusCode = 200
  $ctx.Response.ContentType = Get-ContentType $fsPath
  $ctx.Response.AddHeader('Cache-Control','no-cache')
  $ctx.Response.OutputStream.Write($bytes,0,$bytes.Length)
  $ctx.Response.Close()
}

function Run-Jobs([string]$tool,[string]$query,[int]$topn){
  $ok=$true; $errs=@()
  # unique temp logs to avoid Start-Process error when redirecting to the same target
  $ts = [DateTime]::UtcNow.ToString('yyyyMMdd_HHmmss_fff')
  $logDir = $env:TEMP
  if (-not (Test-Path $logDir)) { $logDir = '.' }
  if ($tool -in @('hadoop','both')){
    $hout = Join-Path $logDir ("ui_run_hadoop_${query}_${ts}_out.log")
    $herr = Join-Path $logDir ("ui_run_hadoop_${query}_${ts}_err.log")
    $reuseArg = @('-Reuse','True')
    $p = Start-Process -FilePath 'powershell' -ArgumentList (@('-ExecutionPolicy','Bypass','-File','scripts/run_hadoop_streaming.ps1','-Only',$query,'-TopN',$topn) + $reuseArg) -NoNewWindow -PassThru -Wait -RedirectStandardOutput $hout -RedirectStandardError $herr
    if ($p.ExitCode -ne 0){ $ok=$false; $errs+="hadoop exit $($p.ExitCode)" }
  }
  if ($tool -in @('pyspark','both')){
    $sout = Join-Path $logDir ("ui_run_pyspark_${query}_${ts}_out.log")
    $serr = Join-Path $logDir ("ui_run_pyspark_${query}_${ts}_err.log")
    $reuseArg = @('-Reuse','True')
    $p = Start-Process -FilePath 'powershell' -ArgumentList (@('-ExecutionPolicy','Bypass','-File','scripts/run_spark_cluster.ps1','-Only',$query,'-TopN',$topn) + $reuseArg) -NoNewWindow -PassThru -Wait -RedirectStandardOutput $sout -RedirectStandardError $serr
    if ($p.ExitCode -ne 0){ $ok=$false; $errs+="pyspark exit $($p.ExitCode)" }
  }
  # Produce JSON for UI
  Start-Process -FilePath 'powershell' -ArgumentList @('-ExecutionPolicy','Bypass','-File','scripts/generate_web_results.ps1') -NoNewWindow -Wait | Out-Null
  return @{ ok = $ok; errors = $errs }
}

# Try to bind on requested port; if busy, try next ports up to +20
$listener = [System.Net.HttpListener]::new()
$bound = $false
$lastErr = $null
for ($p = $Port; $p -le ($Port + 200); $p++) {
  try {
    try { netsh http add urlacl url="http://+:$p/" user="$env:UserName" | Out-Null } catch {}
    $listener.Prefixes.Clear()
    $listener.Prefixes.Add("http://127.0.0.1:$p/")
    $listener.Prefixes.Add("http://localhost:$p/")
    $listener.Start()
    $Port = $p
    $bound = $true
    break
  } catch {
    $lastErr = $_.Exception
    continue
  }
}
if (-not $bound) {
  $msg = "Impossibile avviare il server UI: nessuna porta libera tra $Port..$($Port+200)."
  if ($lastErr -and ($lastErr.Message -match 'denied|negato')) {
    $msg += " Potrebbe essere un problema di permessi URL ACL. Apri PowerShell come Amministratore e lancia: `n  netsh http add urlacl url=http://localhost:$Port/ user=$env:UserName"
  } else {
    $msg += " Verifica chi usa le porte: 'netstat -ano | findstr :$Port' e prova con '-Port 9000'."
  }
  throw $msg
}
Write-Host "Serving UI on http://localhost:$Port/ (Press Ctrl+C to stop)" -ForegroundColor Green

while ($listener.IsListening){
  $ctx = $listener.GetContext()
  try{
    $req = $ctx.Request
    $path = $req.Url.AbsolutePath
    # CORS preflight
    if ($req.HttpMethod -eq 'OPTIONS'){
      $ctx.Response.AddHeader('Access-Control-Allow-Origin','*')
      $ctx.Response.AddHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS')
      $ctx.Response.AddHeader('Access-Control-Allow-Headers','Content-Type')
      $ctx.Response.StatusCode = 204; $ctx.Response.Close(); continue
    }
    if ($path -eq '/' -or $path -eq ''){ Send-File $ctx (Join-Path 'web' 'hadoop_pyspark_results.html'); continue }
    if ($path -eq '/api/health'){ Send-Json $ctx @{ ok = $true }; continue }
    if ($path -eq '/api/run' -and $req.HttpMethod -eq 'POST'){
      $sr = New-Object IO.StreamReader($req.InputStream, [Text.Encoding]::UTF8)
      $body = $sr.ReadToEnd()
      $sr.Close()
      Write-Host "[api/run] body: $body"
      try{ $obj = $body | ConvertFrom-Json } catch { Send-Json $ctx @{ ok=$false; error='Bad JSON' } 400; continue }
      $tool = ($obj.tool | ForEach-Object { $_.ToString().ToLower() })
      $query = ($obj.query | ForEach-Object { $_.ToString().ToLower() })
      $topn = [int]($obj.topn | ForEach-Object { $_ })
      if (-not $topn) { $topn = 3 }
      if ($tool -notin @('hadoop','pyspark','both')){ Send-Json $ctx @{ ok=$false; error='Invalid tool' } 400; continue }
      if ($query -notin @('q1','q2','q3')){ Send-Json $ctx @{ ok=$false; error='Invalid query' } 400; continue }
      Write-Host "[api/run] tool=$tool query=$query topn=$topn"
      $res = Run-Jobs -tool $tool -query $query -topn $topn
      Write-Host "[api/run] result ok=$($res.ok) errors=$($res.errors -join ', ')"
      Send-Json $ctx $res
      continue
    }
    # static files under web/
    $safe = $path.TrimStart('/') -replace '\\','/'
    if ($safe -match '\.\.') { Send-Json $ctx @{ ok=$false; error='Forbidden' } 403; continue }
    Send-File $ctx (Join-Path 'web' $safe)
  } catch {
    try { Send-Json $ctx @{ ok=$false; error=$_.Exception.Message } 500 } catch {}
  }
}
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
