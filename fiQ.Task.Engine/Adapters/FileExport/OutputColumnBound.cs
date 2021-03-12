using System;

namespace fiQ.TaskAdapters.FileExport
{
	/// <summary>
	/// Container class for explicit output behavior of the column at the specified Ordinal (built
	/// from metadata of actual result set, combined with OutputColumnTemplate if present)
	/// </summary>
	internal class OutputColumnBound
	{
		#region Fields
		public string ColumnName { get; init; }
		public int Ordinal { get; init; }
		public Type ColumnType { get; init; }
		public Type ProviderType { get; init; }
		public Format FormatMethod { get; init; }
		public string FormatString { get; init; }
		#endregion
	}
}
