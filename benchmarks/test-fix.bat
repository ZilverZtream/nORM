@echo off
echo 🔬 Testing nORM ID Auto-Generation Fix
echo ======================================

cd /d "%~dp0"

echo Building project...
dotnet build -c Release --no-restore

if %ERRORLEVEL% neq 0 (
    echo ❌ Build failed
    exit /b 1
)

echo.
echo Testing quick functionality...
dotnet run -c Release -- --quick

echo.
echo Test completed!
pause
