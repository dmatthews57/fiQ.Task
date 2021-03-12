namespace fiQ.TaskAdapters.FileExport
{
	/// <summary>
	/// Container class for a file ready to be exported, as returned by queue check procedure
	/// </summary>
	internal class QueuedFile
	{
		/// <summary>
		/// FileID value for this specific file (if NULL, default/parameter value will be used)
		/// </summary>
		public int? FileID { get; init; } = null;
		/// <summary>
		/// Optional Subfolder (under configured destination folder) for file output
		/// </summary>
		public string Subfolder { get; init; } = null;
	}
}
