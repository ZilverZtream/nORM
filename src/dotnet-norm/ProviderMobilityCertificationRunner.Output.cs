using System;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace nORM.Cli;

public static partial class ProviderMobilityCertificationRunner
{
    private static void WriteJson(string reportPath, ProviderMobilityCertificationReport report)
    {
        var fullPath = Path.GetFullPath(reportPath);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var options = new JsonSerializerOptions { WriteIndented = true };
        options.Converters.Add(new JsonStringEnumConverter());
        File.WriteAllText(fullPath, JsonSerializer.Serialize(report, options));
    }

    private static void WriteHtml(string reportPath, ProviderMobilityCertificationReport report)
    {
        var fullPath = Path.GetFullPath(reportPath);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var statusClass = report.Status == "PASS" ? "pass" : "fail";
        var builder = new StringBuilder();
        builder.AppendLine("<!doctype html>");
        builder.AppendLine("<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
        builder.AppendLine("<title>nORM Provider Mobility Certification</title>");
        builder.AppendLine("<style>");
        builder.AppendLine("body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f7f8fa;color:#1f2933}main{max-width:1120px;margin:0 auto;padding:32px}h1{font-size:28px;margin:0 0 6px}.meta{color:#52606d}.summary{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:24px 0}.tile{background:#fff;border:1px solid #dde3ea;border-radius:8px;padding:16px}.tile b{display:block;font-size:24px}.pass{color:#087f5b}.fail{color:#c92a2a}table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #dde3ea;border-radius:8px;overflow:hidden}th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #e6ebf1;vertical-align:top}th{background:#eef2f6;font-size:13px}code{background:#eef2f6;padding:2px 4px;border-radius:4px}.empty{background:#fff;border:1px solid #dde3ea;border-radius:8px;padding:20px}@media(max-width:760px){main{padding:20px}.summary{grid-template-columns:repeat(2,minmax(0,1fr))}}");
        builder.AppendLine("</style></head><body><main>");
        builder.AppendLine("<h1>nORM Provider Mobility Certification</h1>");
        builder.Append("<div class=\"meta\">").Append(Html(report.Contract)).Append(" | profile ").Append(Html(report.Profile)).Append(" | ").Append(Html(report.GeneratedUtc.ToString("O"))).AppendLine("</div>");
        builder.Append("<section class=\"summary\"><div class=\"tile\"><span>Status</span><b class=\"").Append(statusClass).Append("\">").Append(Html(report.Status)).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Errors</span><b>").Append(report.ErrorCount).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Warnings</span><b>").Append(report.WarningCount).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Files</span><b>").Append(report.ScannedFiles).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Schema tables</span><b>").Append(report.SchemaTables).Append("</b></div></section>");
        builder.Append("<p class=\"meta\">Scan path: <code>").Append(Html(report.ScanPath)).AppendLine("</code></p>");

        AppendProviderTargets(builder, report);
        AppendRecommendations(builder, report);
        AppendFindings(builder, report);

        builder.AppendLine("</main></body></html>");
        File.WriteAllText(fullPath, builder.ToString());
    }

    private static string Html(string value) => WebUtility.HtmlEncode(value);
}
