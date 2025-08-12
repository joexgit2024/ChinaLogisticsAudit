# List all VS Code extensions and categorize them
# This script will help you identify which extensions you have installed
# and which ones might be safely disabled

# Function to check if an extension is disabled
function Test-ExtensionDisabled {
    param (
        [string]$extensionId,
        $settingsObject
    )

    if ($null -eq $settingsObject -or 
        -not ($settingsObject.PSObject.Properties.Name -contains "extensions.disabledExtensions") -or
        $null -eq $settingsObject.'extensions.disabledExtensions') {
        return $false
    }

    foreach ($disabledExt in $settingsObject.'extensions.disabledExtensions') {
        if ($disabledExt.id -eq $extensionId) {
            return $true
        }
    }

    return $false
}

# Load VS Code settings
$settingsPath = "$env:APPDATA\Code\User\settings.json"
$settings = $null

if (Test-Path $settingsPath) {
    try {
        $settings = Get-Content $settingsPath -Raw | ConvertFrom-Json
    }
    catch {
        Write-Host "Error reading settings file: $_" -ForegroundColor Red
    }
}

# Get all installed extensions
Write-Host "Fetching installed VS Code extensions..." -ForegroundColor Cyan
$installedExtensions = @(& code --list-extensions)

# Categories of extensions
$pythonExtensions = @(
    "ms-python.python", 
    "ms-python.vscode-pylance", 
    "ms-python.black-formatter", 
    "ms-python.flake8", 
    "ms-python.isort",
    "njpwerner.autodocstring"
)

$databaseExtensions = @(
    "alexcvzz.vscode-sqlite",
    "mtxr.sqltools",
    "mtxr.sqltools-driver-sqlite",
    "adpyke.vscode-sql-formatter",
    "ckolkman.vscode-postgres"
)

$dataScience = @(
    "ms-toolsai.jupyter",
    "ms-toolsai.jupyter-keymap",
    "ms-toolsai.jupyter-renderers",
    "ms-toolsai.datawrangler",
    "ms-toolsai.python-ds-extension-pack",
    "ms-toolsai.vscode-jupyter-cell-tags",
    "ms-toolsai.vscode-jupyter-slideshow"
)

$aiTools = @(
    "github.copilot",
    "github.copilot-chat",
    "sbsnippets.pytorch-snippets",
    "vahidk.tensorflow-snippets"
)

$webDev = @(
    "octref.vetur",
    "esbenp.prettier-vscode"
)

$remoteTools = @(
    "ms-vscode-remote.remote-containers",
    "ms-vscode-remote.remote-ssh",
    "ms-vscode-remote.remote-ssh-edit",
    "ms-vscode-remote.remote-wsl",
    "ms-vscode.remote-explorer"
)

$collaboration = @(
    "ms-vsliveshare.vsliveshare"
)

$containerTools = @(
    "ms-azuretools.vscode-docker",
    "ms-azuretools.vscode-containers"
)

# Print extensions by category
function Write-CategoryExtensions {
    param (
        [string]$categoryName,
        [array]$categoryExtensions,
        [array]$allExtensions
    )

    $found = $false
    Write-Host "`n${categoryName}:" -ForegroundColor Cyan
    
    foreach ($ext in $categoryExtensions) {
        if ($allExtensions -contains $ext) {
            $found = $true
            $status = if (Test-ExtensionDisabled -extensionId $ext -settingsObject $settings) { "DISABLED" } else { "Enabled" }
            $color = if ($status -eq "DISABLED") { "Yellow" } else { "White" }
            Write-Host "- $ext ($status)" -ForegroundColor $color
        }
    }
    
    if (-not $found) {
        Write-Host "- None installed" -ForegroundColor DarkGray
    }
}

# Print all categories
Write-Host "`n===== INSTALLED VS CODE EXTENSIONS =====`n" -ForegroundColor Green
Write-Host "Total extensions installed: $($installedExtensions.Count)" -ForegroundColor White

Write-CategoryExtensions -categoryName "Python Development" -categoryExtensions $pythonExtensions -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "Database Tools" -categoryExtensions $databaseExtensions -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "Data Science Tools" -categoryExtensions $dataScience -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "AI & ML Tools" -categoryExtensions $aiTools -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "Web Development" -categoryExtensions $webDev -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "Remote Development" -categoryExtensions $remoteTools -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "Collaboration Tools" -categoryExtensions $collaboration -allExtensions $installedExtensions
Write-CategoryExtensions -categoryName "Container Tools" -categoryExtensions $containerTools -allExtensions $installedExtensions

# Find uncategorized extensions
$categorizedExtensions = $pythonExtensions + $databaseExtensions + $dataScience + $aiTools + $webDev + $remoteTools + $collaboration + $containerTools
$uncategorized = $installedExtensions | Where-Object { $categorizedExtensions -notcontains $_ }

if ($uncategorized.Count -gt 0) {
    Write-Host "`nOther Extensions:" -ForegroundColor Cyan
    foreach ($ext in $uncategorized) {
        $status = if (Test-ExtensionDisabled -extensionId $ext -settingsObject $settings) { "DISABLED" } else { "Enabled" }
        $color = if ($status -eq "DISABLED") { "Yellow" } else { "White" }
        Write-Host "- $ext ($status)" -ForegroundColor $color
    }
}

Write-Host "`n===== RECOMMENDATION =====`n" -ForegroundColor Green
Write-Host "For your Python/Flask project, consider keeping these essential extensions enabled:" -ForegroundColor White
Write-Host "- Python support: ms-python.python, ms-python.vscode-pylance" -ForegroundColor White
Write-Host "- Database tools: alexcvzz.vscode-sqlite, mtxr.sqltools" -ForegroundColor White
Write-Host "- AI assistance: github.copilot, github.copilot-chat" -ForegroundColor White
Write-Host "`nConsider disabling extensions related to:" -ForegroundColor Yellow
Write-Host "- Data science (if not analyzing data)" -ForegroundColor Yellow
Write-Host "- Remote development (if working locally)" -ForegroundColor Yellow
Write-Host "- Web development (if not working on frontend)" -ForegroundColor Yellow
Write-Host "- Container tools (if not using Docker)" -ForegroundColor Yellow

Write-Host "`n===== ACTIONS =====`n" -ForegroundColor Green
Write-Host "To disable unused extensions: Run 'disable_unused_extensions.ps1'" -ForegroundColor Cyan
Write-Host "To re-enable all extensions: Run 'enable_all_extensions.ps1'" -ForegroundColor Cyan
