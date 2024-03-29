﻿using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace fiQ.TaskUtilities
{
	public static class General
	{
		#region Public fields
		public static readonly Regex REGEX_GUID = new Regex(@"^(\{)?[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\})?$");
		public static readonly Regex REGEX_DATE_MACRO = new Regex(@"<U?(yy|MM|dd|HH|hh|H|h|mm|ss)+([[][+-]\d*[yMdHhms][]])*>");
		public static readonly Regex REGEX_NESTEDPARM_MACRO = new Regex(@"<@(?<name>[^>]+)>");
		/// <summary>
		/// Validate directory/folder path: optional drive [i.e. "C:"] or server share [i.e. "\\server\public" or "\\server\private$"] (if present,
		/// drive or server share must be followed by slash or end of string) followed by any number of subfolders (each must end with slash or end
		/// of string); note that a blank string WILL match this regex, caller must decide whether to reject or treat as "current" folder
		/// </summary>
		public static readonly Regex REGEX_FOLDERPATH = new Regex(@"^(?:(?:[a-zA-Z]\:|[\\/][\\/]+[\w\-.]+[\\/]+[\w\-. ]+\$?)?(?:[\\/]+|$))?(?:[\w\-. <>\[\]]+(?:[\\/]+|$))*$");
		public static readonly Regex REGEX_PATH_SLASHES = new Regex(@"(?<!^)/+"); // All forward slashes except at start of string
		public static readonly Regex REGEX_EMAIL = new Regex(@"^([A-Za-z0-9]((\.(?!\.))|[A-Za-z0-9_+-])*)(?<=[A-Za-z0-9_-])@([A-Za-z0-9][A-Za-z0-9-]*(?<=[A-Za-z0-9])\.)+[A-Za-z0-9][A-Za-z0-9-]{0,22}(?<=[A-Za-z0-9])$");
		/// <summary>
		/// One or more email addresses linked by comma or semicolon; for simplicity re-uses REGEX_EMAIL (stripping start/end anchors)
		/// </summary>
		public static readonly Regex REGEX_EMAIL_LIST = new Regex($"^{REGEX_EMAIL.ToString()[1..^1]}(\\s*[,;]\\s*{REGEX_EMAIL.ToString()[1..^1]})*$");
		#endregion

		#region Private fields
		private static readonly Regex REGEX_DATE_MACRO_FORMAT = new Regex("(yy|MM|dd|HH|hh|H|h|mm|ss)+");
		private static readonly Regex REGEX_DATE_MACRO_ADJUST = new Regex(@"[[](?<sign>[+-])(?<digits>\d*)(?<unit>[yMdHhms])[]]");
		#endregion

		#region Regex and file path utility methods
		/// <summary>
		/// Create a Regex object from the specified string and options
		/// </summary>
		/// <returns>null if string is null or empty, Regex otherwise</returns>
		public static Regex RegexIfPresent(string regex, RegexOptions regexoptions = RegexOptions.None)
		{
			return string.IsNullOrEmpty(regex) ? null : new Regex(regex, regexoptions);
		}

		/// <summary>
		/// Convert file filter to filename validation regex
		/// </summary>
		/// <returns>null if string is null or empty, case-insensitive Regex otherwise</returns>
		public static Regex RegexFromFileFilter(string fileFilter)
		{
			// Escape regex-reserved characters, then convert escaped wildcards (* and ?) to their regex equivalents:
			return string.IsNullOrEmpty(fileFilter) ? null
				: new Regex($"^{Regex.Escape(fileFilter).Replace(@"\*", ".*").Replace(@"\?", ".")}$", RegexOptions.IgnoreCase);
		}

		/// <summary>
		/// Combine portions of path together (ignoring empty entries) and standardize with forward slashes
		/// </summary>
		/// <remarks>
		/// Unlike Path.Combine, will not allow leading slash in entries after the first to override entire path. Will replace
		/// any consecutive slashes (after the first character, to allow network share path) with single slash
		/// </remarks>
		public static string PathCombine(params string[] values)
		{
			return REGEX_PATH_SLASHES.Replace(string.Join('/', values.Where(s => !string.IsNullOrEmpty(s))).Replace('\\', '/'), "/");
		}
		#endregion

		#region Exception management methods
		/// <summary>
		/// Simplify asynchronous exceptions (async function calls throw AggregateException even if
		/// only one exception actually occurred; extract single Exception object in this case)
		/// </summary>
		public static Exception SimplifyAggregateException(AggregateException ae)
		{
			Exception AggregateEx = null, SingleEx = null;

			// AggregateException's handler function will execute for each exception in collection;
			// if there is only one exception here, AggregateEx will stay null:
			ae.Handle(ex =>
			{
				if (SingleEx == null)
				{
					SingleEx = ex;
				}
				else
				{
					AggregateEx = ae;
				}
				return true;
			});

			// If AggregateEx is not null there are multiple exceptions, otherwise there is only one:
			return AggregateEx ?? SingleEx;
		}
		#endregion

		#region Macro processing methods
		/// <summary>
		/// Dynamically replace all instances of date/time macro strings (complying with REGEX_DATE_MACRO: some combination of
		/// year/month/day/hour/minute/second placeholders contained in "<>" brackets and optionally adjusted by a value in
		/// "[]" brackets) with the specified date/time (adjusted as appropriate), formatting as specified by macro
		/// </summary>
		public static string ApplyDateMacros(string macro, DateTime applyDateTime)
		{
			if (string.IsNullOrEmpty(macro))
			{
				return macro;
			}

			// Input string may contain multiple macro instances; pull all distinct matches from string for processing (any
			// repeated macro only needs to be processed once, as all instances in the original string will be replaced):
			var matches = REGEX_DATE_MACRO.Matches(macro)
				.Cast<Match>()
				.Select(m => m.Value)
				.Distinct();
			foreach (var match in matches)
			{
				// If macro string starts with a "U", convert time to UTC:
				var dt = match[0] == 'U' ? applyDateTime.ToUniversalTime() : applyDateTime;

				#region Apply date adjustments
				// Locate optional trailing time adjustment values contained in "[]" (for example, "[-2M][-d]" would deduct
				// two months and one day from current time), and apply adjustments to DateTime value
				var adjustments = REGEX_DATE_MACRO_ADJUST.Matches(match).Cast<Match>();
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
							dt = dt.AddYears(numAdj);
							break;
						case "M":
							dt = dt.AddMonths(numAdj);
							break;
						case "d":
							dt = dt.AddDays(numAdj);
							break;
						case "H":
						case "h":
							dt = dt.AddHours(numAdj);
							break;
						case "m":
							dt = dt.AddMinutes(numAdj);
							break;
						case "s":
							dt = dt.AddSeconds(numAdj);
							break;
					};
				}
				#endregion

				// Extract only the formatting portion of match string:
				string customFormat = REGEX_DATE_MACRO_FORMAT.Match(match).Value;

				// Replace all instances of the matched macro string with formatted date/time string (note that single-character
				// format strings "H" and "h" are allowed by regex, these must be passed to ToString as "%H" or "%h"):
				macro = macro.Replace(match, dt.ToString(customFormat switch { "H" => "%H", "h" => "%h", _ => customFormat }));
			}

			// Any macros present in string have now been replaced, return result
			return macro;
		}
		#endregion
	}
}
