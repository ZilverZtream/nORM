@echo off
echo 🔧 Quick Build Test
echo ===================

cd /d "%~dp0"

echo Building nORM core...
cd ..\src
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ nORM build failed
    goto :end
)

echo.
echo Building benchmark project...
cd ..\benchmarks
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ Benchmark build failed
    goto :end
)

echo.
echo ✅ Build successful!
echo.
echo Running quick JOIN verification...
dotnet run -c Release -- --quick

:end
pause
