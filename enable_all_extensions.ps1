# Enable all VS Code extensions
# Run this script if you want to re-enable all extensions

$settingsPath = "$env:APPDATA\Code\User\settings.json"

if (Test-Path $settingsPath) {
    try {
        $settings = Get-Content $settingsPath -Raw | ConvertFrom-Json
        
        # Check if we have disabled extensions
        if (($settings.PSObject.Properties.Name -contains "extensions.disabledExtensions") -and 
            ($settings.'extensions.disabledExtensions'.Count -gt 0)) {
            
            $disabledCount = $settings.'extensions.disabledExtensions'.Count
            
            # Clear the disabled extensions list
            $settings.'extensions.disabledExtensions' = @()
            
            # Save the settings
            $settings | ConvertTo-Json -Depth 10 | Set-Content $settingsPath
            
            Write-Host "$disabledCount extensions have been enabled." -ForegroundColor Green
            Write-Host "Please restart VS Code for the changes to take effect." -ForegroundColor Cyan
        }
        else {
            Write-Host "No disabled extensions found in settings." -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "Error reading or updating settings file: $_" -ForegroundColor Red
    }
}
else {
    Write-Host "VS Code settings file not found at: $settingsPath" -ForegroundColor Red
}
