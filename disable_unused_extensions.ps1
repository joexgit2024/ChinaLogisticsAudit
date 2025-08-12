# Disable unused VS Code extensions to save memory
# Run this script in PowerShell to disable extensions not currently needed

# Extensions to disable (not actively used in DHL invoice audit project)
$extensionsToDisable = @(
    # Docker/containers - not used in current project
    "ms-azuretools.vscode-containers",
    "ms-azuretools.vscode-docker",
    
    # Data science extensions - not used in invoice audit app
    "ms-toolsai.datawrangler",
    "ms-toolsai.jupyter",
    "ms-toolsai.jupyter-keymap",
    "ms-toolsai.jupyter-renderers",
    "ms-toolsai.python-ds-extension-pack",
    "ms-toolsai.vscode-jupyter-cell-tags",
    "ms-toolsai.vscode-jupyter-slideshow",
    
    # AI/ML extensions - not used in invoice processing
    "sbsnippets.pytorch-snippets",
    "vahidk.tensorflow-snippets",
    
    # Remote development - not needed for local development
    "ms-vscode-remote.remote-containers",
    "ms-vscode-remote.remote-ssh",
    "ms-vscode-remote.remote-ssh-edit",
    "ms-vscode-remote.remote-wsl",
    "ms-vscode.remote-explorer",
    
    # Collaboration tools - not needed if working solo
    "ms-vsliveshare.vsliveshare",
    
    # Web development - not used in Python backend project
    "octref.vetur", # Vue.js
    "esbenp.prettier-vscode", # JavaScript/web formatter
    
    # Unused database tools - keep only what you need
    "ckolkman.vscode-postgres", # Keep only if using PostgreSQL
    
    # Miscellaneous
    "ms-vscode.vscode-speech",
    "ms-vscode.powershell" # Keep if you frequently use PowerShell
)

Write-Host "The following extensions will be disabled:" -ForegroundColor Yellow
foreach ($extension in $extensionsToDisable) {
    Write-Host "- $extension"
}

$confirmation = Read-Host "`nDo you want to proceed? (y/n)"
if ($confirmation -ne 'y') {
    Write-Host "Operation cancelled." -ForegroundColor Red
    exit
}

# Create or update the disabled extensions list in VS Code settings
$settingsPath = "$env:APPDATA\Code\User\settings.json"

if (Test-Path $settingsPath) {
    try {
        $settings = Get-Content $settingsPath -Raw | ConvertFrom-Json
    }
    catch {
        $settings = [PSCustomObject]@{}
    }
}
else {
    $settings = [PSCustomObject]@{}
}

# Convert to PSCustomObject if it's not already
if ($null -eq $settings) {
    $settings = [PSCustomObject]@{}
}

# Ensure we have a disabledExtensions property as array
if (-not ($settings.PSObject.Properties.Name -contains "extensions.ignoreRecommendations")) {
    Add-Member -InputObject $settings -MemberType NoteProperty -Name "extensions.ignoreRecommendations" -Value $true
}

# Ensure we have a disabledExtensions property as array
if (-not ($settings.PSObject.Properties.Name -contains "extensions.disableExtensions")) {
    Add-Member -InputObject $settings -MemberType NoteProperty -Name "extensions.disableExtensions" -Value $true
}

# Create disabledExtensions array if it doesn't exist
if (-not ($settings.PSObject.Properties.Name -contains "extensions.disabledExtensions")) {
    Add-Member -InputObject $settings -MemberType NoteProperty -Name "extensions.disabledExtensions" -Value @()
}
elseif ($null -eq $settings.'extensions.disabledExtensions') {
    $settings.'extensions.disabledExtensions' = @()
}

# Convert to array if it's not already
if ($settings.'extensions.disabledExtensions' -isnot [Array]) {
    $settings.'extensions.disabledExtensions' = @($settings.'extensions.disabledExtensions')
}

# Add extensions to disable list
$disabledList = [System.Collections.ArrayList]@()
$extensionsAdded = 0

# Check each extension to see if it's already disabled
foreach ($extension in $extensionsToDisable) {
    $alreadyDisabled = $false
    
    # Check if extension is already in the disabled list
    if ($settings.'extensions.disabledExtensions'.Count -gt 0) {
        foreach ($disabledExt in $settings.'extensions.disabledExtensions') {
            if ($disabledExt.id -eq $extension) {
                $alreadyDisabled = $true
                break
            }
        }
    }
    
    # Add to disabled list if not already there
    if (-not $alreadyDisabled) {
        $disabledExt = [PSCustomObject]@{
            id = $extension
        }
        $disabledList.Add($disabledExt) | Out-Null
        $extensionsAdded++
    }
}

# Update the settings
$settings.'extensions.disabledExtensions' = $disabledList

# Save the settings
$settings | ConvertTo-Json -Depth 10 | Set-Content $settingsPath

Write-Host "`n$extensionsAdded extensions have been added to the disabled list." -ForegroundColor Green
Write-Host "Please restart VS Code for the changes to take effect." -ForegroundColor Cyan
Write-Host "`nTo re-enable extensions later, go to:" -ForegroundColor White
Write-Host "VS Code > Manage > Extensions > Show Disabled Extensions" -ForegroundColor White
