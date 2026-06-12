using System.Diagnostics;
using System.IO;

partial class Program
{
    /// <summary>
    /// Resolves the build output assembly path for a project by running
    /// <c>dotnet msbuild -getProperty:TargetPath</c> on the project file.
    /// Returns null if the path cannot be determined.
    /// </summary>
    static string? ResolveAssemblyFromProject(
        string projectPath,
        string? targetFramework = null,
        string? configuration = null,
        string? runtime = null)
    {
        try
        {
            var startInfo = new ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            startInfo.ArgumentList.Add("msbuild");
            startInfo.ArgumentList.Add(projectPath);
            AddMSBuildProperty(startInfo, "TargetFramework", targetFramework);
            AddMSBuildProperty(startInfo, "Configuration", configuration);
            AddMSBuildProperty(startInfo, "RuntimeIdentifier", runtime);
            startInfo.ArgumentList.Add("-getProperty:TargetPath");
            startInfo.ArgumentList.Add("--verbosity:quiet");

            using var proc = Process.Start(startInfo);
            if (proc == null) return null;
            var output = proc.StandardOutput.ReadToEnd().Trim();
            proc.WaitForExit();
            if (proc.ExitCode == 0 && !string.IsNullOrWhiteSpace(output) && File.Exists(output))
                return output;
            return null;
        }
        catch
        {
            return null;
        }
    }

    static void AddMSBuildProperty(ProcessStartInfo startInfo, string propertyName, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            startInfo.ArgumentList.Add($"-property:{propertyName}={value}");
    }
}
