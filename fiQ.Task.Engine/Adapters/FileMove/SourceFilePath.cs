namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Container class for combination of path, file filter and optional regex of files to be moved
	/// </summary>
	internal class SourceFilePath
	{
		public string FolderPath { get; init; }
		public string FilenameFilter { get; init; }
		public string FilenameRegex { get; init; }
		public string DestinationSubfolder { get; init; }

		public bool Invalid()
		{
			return string.IsNullOrEmpty(FilenameFilter);
		}
	}
}
