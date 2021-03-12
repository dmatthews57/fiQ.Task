namespace fiQ.TaskAdapters.FileExport
{
	/// <summary>
	/// Enumeration for methods of formatting column values in exported file
	/// </summary>
	internal enum Format
	{
		Exclude,    // Exclude this column from output
		Auto,       // Dynamically format columns into strings based on column type
		Raw,        // Dump column values directly to file using ToString
		Explicit    // Use string.Format(FormatString) or [cell].ToString(FormatString) to generate output
	};
}
