using System;
using System.Collections.Generic;

namespace fiQ.TaskAdapters.FileMove
{
	class DownloadFileList
	{
		/// <summary>
		/// Collection of data on previously-downloaded files (for persisting record of previous runs)
		/// </summary>
		public HashSet<DownloadFile> downloadFiles { get; set; } = new();

		/// <summary>
		/// Prune listing by removing files older than the specified TimeSpan (if provided)
		/// </summary>
		/// <returns>true if list was modified, false otherwise</returns>
		public bool PruneList(TimeSpan? ts)
		{
			if ((ts?.TotalMilliseconds ?? 0) != 0)
			{
				var minTime = DateTime.Now.Add(((TimeSpan)ts).TotalMilliseconds > 0 ? ((TimeSpan)ts).Negate() : (TimeSpan)ts);
				return downloadFiles.RemoveWhere(file => file.downloadedAt < minTime) > 0;
			}
			else
			{
				return false;
			}
		}
	}
}
