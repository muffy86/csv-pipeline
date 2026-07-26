#!/usr/bin/env pwsh
<#
.SYNOPSIS
    CSV Pipeline Processor — Multi-stage data pipeline
.DESCRIPTION
    Processes CSV files through a 4-stage pipeline:
    1. Ingest: Read raw CSV(s) from incoming/ directory
    2. Validate: Check for missing values, data types
    3. Transform: Clean, normalize, enrich data
    4. Export: Output to CSV, JSON, and generate summary report
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$InputDir = ".\incoming",
    [Parameter(Mandatory=$false)]
    [string]$OutputDir = ".\processed",
    [Parameter(Mandatory=$false)]
    [string]$ArchiveDir = ".\archive",
    [Parameter(Mandatory=$false)]
    [switch]$Watch,
    [Parameter(Mandatory=$false)]
    [int]$WatchInterval = 10
)

function Invoke-Pipeline {
    param([string]$FilePath)

    Write-Host "`n=========================================" -ForegroundColor Magenta
    Write-Host "PROCESSING: $FilePath" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Magenta

    # ── STAGE 1: Ingest ──────────────────────────────────────
    Write-Host "`n[STAGE 1/4] Ingest" -ForegroundColor Yellow
    try {
        $data = Import-Csv -Path $FilePath -ErrorAction Stop
        Write-Host "  ✓ Loaded $($data.Count) rows from $([System.IO.Path]::GetFileName($FilePath))" -ForegroundColor Green
    } catch {
        Write-Error "  ✗ Failed to read CSV: $_"
        return $false
    }

    if ($data.Count -eq 0) {
        Write-Warning "  ⚠ Empty CSV file — skipping"
        return $false
    }

    # ── STAGE 2: Validate ────────────────────────────────────
    Write-Host "`n[STAGE 2/4] Validate" -ForegroundColor Yellow
    $columns = $data[0].PSObject.Properties.Name
    Write-Host "  Columns detected: $($columns -join ', ')"
    
    $issues = @()
    $totalNulls = 0
    foreach ($row in $data) {
        foreach ($col in $columns) {
            if ([string]::IsNullOrWhiteSpace($row.$col)) {
                $totalNulls++
            }
        }
    }
    if ($totalNulls -gt 0) {
        Write-Warning "  ⚠ Found $totalNulls missing values across dataset"
        $issues += "Missing values: $totalNulls"
    } else {
        Write-Host "  ✓ All values present" -ForegroundColor Green
    }

    # ── STAGE 3: Transform ──────────────────────────────────
    Write-Host "`n[STAGE 3/4] Transform" -ForegroundColor Yellow
    
    # Auto-detect numeric columns and ensure proper types
    $transformed = $data | ForEach-Object -Begin { $i = 0 } -Process {
        $row = $_
        $i++
        $row.PSObject.Properties | ForEach-Object {
            $name = $_.Name
            $val = $_.Value
            # Try to convert numeric strings to numbers
            if ($val -and $val -match '^[\d,]+\.?\d*$') {
                $row.$name = $val -replace ',','' -as [double]
            }
            # Trim whitespace
            if ($val -is [string]) {
                $row.$name = $val.Trim()
            }
        }
        # Add metadata columns
        $row | Add-Member -NotePropertyName "processed_at" -NotePropertyValue (Get-Date -Format "yyyy-MM-dd HH:mm:ss") -Force
        $row | Add-Member -NotePropertyName "source_file" -NotePropertyValue ([System.IO.Path]::GetFileName($FilePath)) -Force
        $row
    }
    Write-Host "  ✓ Transformed $($transformed.Count) rows (types normalized, metadata added)" -ForegroundColor Green

    # ── STAGE 4: Export ──────────────────────────────────────
    Write-Host "`n[STAGE 4/4] Export" -ForegroundColor Yellow
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($FilePath)
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

    # Ensure output directory
    New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

    # Export CSV
    $csvPath = Join-Path $OutputDir "${baseName}_processed_${timestamp}.csv"
    $transformed | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
    Write-Host "  ✓ CSV: $csvPath ($((Get-Item $csvPath).Length / 1KB) KB)" -ForegroundColor Green

    # Export JSON
    $jsonPath = Join-Path $OutputDir "${baseName}_processed_${timestamp}.json"
    $transformed | ConvertTo-Json -Depth 5 | Out-File $jsonPath -Encoding UTF8
    Write-Host "  ✓ JSON: $jsonPath" -ForegroundColor Green

    # Generate summary report
    $summary = @{
        PipelineRun = Get-Date -Format "o"
        SourceFile = [System.IO.Path]::GetFileName($FilePath)
        RowsIngested = $data.Count
        RowsExported = $transformed.Count
        Columns = $columns
        ValidationIssues = $issues
        Outputs = @{ CSV = $csvPath; JSON = $jsonPath }
    }
    $summaryPath = Join-Path $OutputDir "${baseName}_summary_${timestamp}.json"
    $summary | ConvertTo-Json -Depth 3 | Out-File $summaryPath -Encoding UTF8
    Write-Host "  ✓ Summary: $summaryPath" -ForegroundColor Green

    # Archive source
    New-Item -ItemType Directory -Force -Path $ArchiveDir | Out-Null
    Move-Item -Path $FilePath -Destination $ArchiveDir -Force
    Write-Host "  ✓ Archived source to $ArchiveDir" -ForegroundColor DarkGray

    # ── Report ───────────────────────────────────────────────
    Write-Host "`n=== PIPELINE RESULT ===" -ForegroundColor Green
    Write-Host "✓ Processed $($data.Count) rows → $($transformed.Count) rows" -ForegroundColor Green
    if ($issues.Count -gt 0) { Write-Warning "⚠ $($issues.Count) validation issues" }

    return $true
}

# ── Main Execution ──────────────────────────────────────
Write-Host @"
┌──────────────────────────────────────┐
│       CSV PIPELINE PROCESSOR         │
│   Stage: Ingest → Validate →         │
│          Transform → Export          │
└──────────────────────────────────────┘
"@ -ForegroundColor Cyan

Write-Host "Input:  $InputDir"
Write-Host "Output: $OutputDir"
Write-Host "Archive: $ArchiveDir"

# Create directories
foreach ($d in @($InputDir, $OutputDir, $ArchiveDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

if ($Watch) {
    Write-Host "`nWatching $InputDir for new CSV files every ${WatchInterval}s... (Ctrl+C to stop)" -ForegroundColor Yellow
    $watcher = New-Object System.IO.FileSystemWatcher
    $watcher.Path = (Resolve-Path $InputDir).Path
    $watcher.Filter = "*.csv"
    $watcher.EnableRaisingEvents = $true
    
    while ($true) {
        $result = Register-ObjectEvent $watcher "Created" -Action {
            Start-Sleep -Seconds 1  # Wait for file to finish writing
            & $MyInvocation.MyCommand.Path -FilePath $Event.SourceEventArgs.FullPath
        } -MessageData $null -SupportEvent
        Start-Sleep -Seconds $WatchInterval
        Get-Event | Remove-Event -ErrorAction SilentlyContinue
    }
} else {
    # Process existing CSV files
    $csvFiles = Get-ChildItem -Path $InputDir -Filter "*.csv"
    if ($csvFiles.Count -eq 0) {
        Write-Host "`nNo CSV files found in $InputDir" -ForegroundColor Yellow
        Write-Host "Drop .csv files into $InputDir and re-run, or use:" -ForegroundColor DarkGray
        Write-Host "  > $PSCommandPath -Watch" -ForegroundColor DarkGray
        Write-Host "`nCreating sample CSV for testing..." -ForegroundColor Cyan
        
        # Create sample data
        @"
Name,Email,Department,Salary,StartDate
Alice Johnson,alice@example.com,Engineering,95000,2023-01-15
Bob Smith,bob@example.com,Marketing,72000,2022-06-01
Charlie Brown,charlie@example.com,Sales,85000,2024-03-10
Diana Prince,diana@example.com,Engineering,105000,2021-09-20
Evan Williams,evan@example.com,HR,65000,2023-11-05
"@ | Out-File (Join-Path $InputDir "employees.csv") -Encoding UTF8
        
        Write-Host "Created sample: $InputDir\employees.csv" -ForegroundColor Green
        Write-Host "Re-run to process it!" -ForegroundColor DarkGray
    } else {
        $results = @()
        foreach ($csv in $csvFiles) {
            $ok = Invoke-Pipeline -FilePath $csv.FullName
            $results += [PSCustomObject]@{ File = $csv.Name; Success = $ok }
        }
        Write-Host "`n=== FINAL SUMMARY ===" -ForegroundColor Magenta
        $results | Format-Table -AutoSize
        $success = ($results | Where-Object Success -eq $true).Count
        $total = $results.Count
        Write-Host "Pipeline complete: $success/$total files processed successfully" -ForegroundColor Green
    }
}
