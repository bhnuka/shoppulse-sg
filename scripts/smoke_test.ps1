param(
  [string]$BaseUrl = "http://localhost:8000"
)

function Assert-NotNull($val, $msg) {
  if ($null -eq $val) { throw $msg }
}

Write-Host "Testing $BaseUrl" -ForegroundColor Cyan

# 1) Health
$health = Invoke-RestMethod -Method GET -Uri "$BaseUrl/health"
Assert-NotNull $health.status "Health: missing status"
Write-Host "Health OK: $($health.status)" -ForegroundColor Green

# 2) Overview
$overview = Invoke-RestMethod -Method GET -Uri "$BaseUrl/api/overview"
Assert-NotNull $overview.period_start "Overview: missing period_start"
Assert-NotNull $overview.period_end "Overview: missing period_end"
Assert-NotNull $overview.metrics "Overview: missing metrics"
Write-Host "Overview OK" -ForegroundColor Green

# 3) Trends
$trends = Invoke-RestMethod -Method GET -Uri "$BaseUrl/api/trends/new-entities"
Assert-NotNull $trends.series "Trends: missing series"
Write-Host "Trends OK (series count: $($trends.series.Count))" -ForegroundColor Green

# 4) Rankings
$rankings = Invoke-RestMethod -Method GET -Uri "$BaseUrl/api/rankings/top-ssic"
Assert-NotNull $rankings.items "Rankings: missing items"
Write-Host "Rankings OK (items: $($rankings.items.Count))" -ForegroundColor Green

# 5) Map Hotspots
$hotspots = Invoke-RestMethod -Method GET -Uri "$BaseUrl/api/map/hotspots"
Assert-NotNull $hotspots.hotspots "Hotspots: missing hotspots"
Write-Host "Hotspots OK (count: $($hotspots.hotspots.Count))" -ForegroundColor Green

# 6) Entity search
$search = Invoke-RestMethod -Method GET -Uri "$BaseUrl/api/entities/search?q=PAE"
Assert-NotNull $search.total "Search: missing total"
Assert-NotNull $search.items "Search: missing items"
Write-Host "Search OK (total: $($search.total))" -ForegroundColor Green

# 7) Entity detail
if ($search.items.Count -gt 0) {
  $uen = $search.items[0].uen
  $detail = Invoke-RestMethod -Method GET -Uri "$BaseUrl/api/entities/$uen"
  Assert-NotNull $detail.uen "Detail: missing uen"
  Write-Host "Entity detail OK ($uen)" -ForegroundColor Green
} else {
  Write-Host "Search returned 0 items; skipping detail" -ForegroundColor Yellow
}

# 8) NL→SQL sql-only
$sqlOnlyBody = @{ question = "Top SSICs in Tampines last 12 months" } | ConvertTo-Json
$sqlOnly = Invoke-RestMethod -Method POST -Uri "$BaseUrl/api/chat/sql-only" -ContentType "application/json" -Body $sqlOnlyBody
Assert-NotNull $sqlOnly.intent "sql-only: missing intent"
Assert-NotNull $sqlOnly.sql "sql-only: missing sql"
Write-Host "NL2SQL sql-only OK" -ForegroundColor Green

# 9) NL→SQL query
$queryBody = @{ question = "Compare Jurong West vs Woodlands for new F&B entities" } | ConvertTo-Json
$queryRes = Invoke-RestMethod -Method POST -Uri "$BaseUrl/api/chat/query" -ContentType "application/json" -Body $queryBody
Assert-NotNull $queryRes.intent "query: missing intent"
Assert-NotNull $queryRes.sql "query: missing sql"
Assert-NotNull $queryRes.data "query: missing data"
Assert-NotNull $queryRes.narrative "query: missing narrative"
Write-Host "NL2SQL query OK" -ForegroundColor Green

Write-Host "All tests passed." -ForegroundColor Cyan
