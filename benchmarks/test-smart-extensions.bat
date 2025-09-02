@echo off
echo 🏎️ Testing Smart nORM Extension Performance Fix
echo ================================================

cd /d "%~dp0"

echo Building nORM core library...
cd ..\src
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ nORM build failed
    exit /b 1
)

echo.
echo Building benchmark project...
cd ..\benchmarks
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ Benchmark build failed
    exit /b 1
)

echo.
echo ✅ Build completed successfully!
echo.
echo 🎯 Testing smart extension method approach...
echo This should avoid EF Core conflicts and maintain performance!
echo.

dotnet run -c Release -- --fast

echo.
echo 🏁 Smart extension test completed!
echo Look for ~39ns query times (should be MUCH faster than 100,000ns)
pause
