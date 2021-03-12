using System.Text.RegularExpressions;

namespace fiQ.TaskAdapters.FileExport
{
	/// <summary>
	/// Configuration template for container class detailing explicit output behavior for a
	/// single column from result set
	/// </summary>
	internal class OutputColumnTemplate
	{
		#region Fields
		private static readonly Regex stringFormatRegex = new Regex(@"^[^{]*{0(,-?\d+)?(:.+)?}[^{]*$"); // string.Format style with single argument and optional formatting

		/// <summary>
		/// Name of column in dataset output by stored procedure
		/// </summary>
		public string ColumnName { get; init; }
		/// <summary>
		/// Optional replacement value for column header in output file
		/// </summary>
		public string OutputNameOverride { get; init; } = null;
		/// <summary>
		/// Method of formatting data to file
		/// </summary>
		public Format FormatMethod { get; init; } = Format.Auto;
		/// <summary>
		/// Format string for Explicit formatting method
		/// </summary>
		public string FormatString { get; init; } = null;
		#endregion

		#region Methods
		public bool IsInvalid()
		{
			// ColumnName is always required (to match up with dataset column name):
			if (string.IsNullOrEmpty(ColumnName))
			{
				return true;
			}
			// If formatting is being done explicitly, format string is required:
			else if (FormatMethod == Format.Explicit)
			{
				if (string.IsNullOrEmpty(FormatString)) return true;
				else if (!stringFormatRegex.IsMatch(FormatString)) return true;
			}
			// Otherwise, column is valid output:
			return false;
		}
		#endregion
	}
}
