using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using fiQ.Task.Adapters;
using Microsoft.Extensions.DependencyInjection;

namespace fiQ.Task.Utilities
{
	public class TaskUtilities
	{
		#region Public fields
		public static readonly Regex REGEX_DATE_MACRO = new Regex(@"<U?(yy|MM|dd|HH|hh|H|h|mm|ss)+([[][+-]\d*[yMdHhms][]])*>");
		public static readonly Regex REGEX_DIRPATH = new Regex(@"^(?:[a-zA-Z]\:|[\\/]{2}[\w\-.]+[\\/][\w\-. ]+\$?)(?:[\\/][\w\-. ]+)*[\\/]?$");
		public static readonly Regex REGEX_EMAIL = new Regex(@"^([A-Za-z0-9]((\.(?!\.))|[A-Za-z0-9_+-])*)(?<=[A-Za-z0-9_-])@([A-Za-z0-9][A-Za-z0-9-]*(?<=[A-Za-z0-9])\.)+[A-Za-z0-9][A-Za-z0-9-]{0,22}(?<=[A-Za-z0-9])$");
		public static readonly Regex REGEX_GUID = new Regex(@"^(\{)?[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\})?$");
		#endregion

		#region Private fields
		private static readonly Regex REGEX_DATE_MACRO_FORMAT = new Regex("(yy|MM|dd|HH|hh|H|h|mm|ss)+");
		private static readonly Regex REGEX_DATE_MACRO_ADJUST = new Regex(@"[[](?<sign>[+-])(?<digits>\d*)(?<unit>[yMdHhms])[]]");
		#endregion

		#region Filename management methods
		/// <summary>
		/// Check whether existing filename exists at specified path; if so, generate a
		/// unique filename by adding an integer suffix (incrementing until not found)
		/// </summary>
		public static string GetNextFilename(string filePath, int maxDuplicates = 100)
		{
			if (File.Exists(filePath))
			{
				for (int i = 0; i < maxDuplicates; i++)
				{
					var testPath = $"{filePath}.{i}";
					if (!File.Exists(testPath))
					{
						return testPath;
					}
				}
				// If this point is reached, maximum duplicate files found - throw exception:
				throw new Exception($"Maximum number of duplicate files at path {filePath}");
			}
			else return filePath;
		}

		/// <summary>
		/// Convert file filter to filename validation regex
		/// </summary>
		/// <returns>A case-insensitive Regex object with the converted regular expression</returns>
		public static Regex RegexFromFileFilter(string fileFilter)
		{
			// Escape regex-reserved characters, then convert escaped wildcards (* and ?) to their regex equivalents:
			return new Regex($"^{Regex.Escape(fileFilter).Replace(@"\*", ".*").Replace(@"\?", ".")}$", RegexOptions.IgnoreCase);
		}
		#endregion

		#region Macro processing methods
		/// <summary>
		/// Dynamically replace all instances of date/time macro strings (complying with REGEX_DATE_MACRO: some combination of
		/// year/month/day/hour/minute/second placeholders contained in "<>" brackets and optionally adjusted by a value in
		/// "[]" brackets) with the current date/time (adjusted as appropriate), formatting as specified by macro
		/// </summary>
		public static string ApplyDateMacros(string macro)
		{
			// Input string may contain multiple macro instances; pull all distinct matches from string for processing (any
			// repeated macro only needs to be processed once, as all instances in the original string will be replaced):
			var matches = REGEX_DATE_MACRO.Matches(macro).OfType<Match>().Select(m => m.Value).Distinct();
			foreach (var match in matches)
			{
				// Use UTC time only if macro string starts with a "U", otherwise use local time:
				var datetime = match.Substring(1, 1) == "U" ? DateTime.UtcNow : DateTime.Now;

				#region Apply date adjustments
				// Locate optional trailing add/remove time values contained in "[]" (for example, "[-2M][-d]" would deduct
				// two months and one day from current time), and apply adjustments to DateTime value
				var adjustments = REGEX_DATE_MACRO_ADJUST.Matches(match).OfType<Match>();
				foreach (var adjust in adjustments)
				{
					// If numeric value is provided, parse - otherwise assume a value of "1":
					if (!int.TryParse(adjust.Groups["digits"].Value, out var numAdj))
					{
						numAdj = 1;
					}
					// Flip value if indicated by sign:
					if (adjust.Groups["sign"].Value == "-")
					{
						numAdj *= -1;
					}

					// Adjust time value based on specified number of specified unit:
					switch (adjust.Groups["unit"].Value)
					{
						case "y":
							datetime = datetime.AddYears(numAdj);
							break;
						case "M":
							datetime = datetime.AddMonths(numAdj);
							break;
						case "d":
							datetime = datetime.AddDays(numAdj);
							break;
						case "H":
						case "h":
							datetime = datetime.AddHours(numAdj);
							break;
						case "m":
							datetime = datetime.AddMinutes(numAdj);
							break;
						case "s":
							datetime = datetime.AddSeconds(numAdj);
							break;
					};
				}
				#endregion

				// Extract only the formatting portion of match string:
				string customFormat = REGEX_DATE_MACRO_FORMAT.Match(match).Value;

				// Replace all instances of the matched macro string with formatted date/time string (note that single-character
				// format strings "H" and "h" are allowed by regex, these must be passed to ToString as "%H" or "%h"):
				macro = macro.Replace(match, datetime.ToString(customFormat switch { "H" => "%H", "h" => "%h", _ => customFormat }));
			}

			// Any macros present in string have now been replaced, return result
			return macro;
		}
		#endregion
	}
}
