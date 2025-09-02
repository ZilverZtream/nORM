@echo off
echo 🔧 Testing nORM JOIN Fix
echo ========================

cd /d "%~dp0"

echo Building project in Release mode...
dotnet build -c Release --verbosity quiet

if %ERRORLEVEL% neq 0 (
    echo ❌ Build failed
    exit /b 1
)

echo ✅ Build successful

echo.
echo Running quick JOIN verification test...
dotnet run -c Release -- --quick

echo.
echo Testing fast benchmarks (should no longer fail on Query_Join)...
dotnet run -c Release -- --fast

echo.
echo 🎉 Test completed!
pause
