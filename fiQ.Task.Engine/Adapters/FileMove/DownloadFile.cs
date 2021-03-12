using System;
using System.Collections.Generic;
using System.IO;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Container class for information on a specific file that has been (or can be) downloaded by FileMoveAdapter
	/// </summary>
	internal class DownloadFile : IEquatable<DownloadFile>
	{
		#region Fields and constructors
		/// <summary>
		/// Destination subfolder to which file will be downloaded (not serialized or included in hash)
		/// </summary>
		private string DestinationSubfolder = null;
		/// <summary>
		/// Source subfolder from which file will be (or was) downloaded
		/// </summary>
		public string fileFolder { get; set; }
		/// <summary>
		/// Filename, without path
		/// </summary>
		public string fileName { get; set; }
		/// <summary>
		/// Date/time file was last written
		/// </summary>
		public DateTime? lastWriteTime { get; set; } = null;
		/// <summary>
		/// Size of file in bytes
		/// </summary>
		public long size { get; set; } = 0;
		/// <summary>
		/// Date file was downloaded (serialized but not part of hash)
		/// </summary>
		public DateTime downloadedAt { get; set; } = DateTime.Now;

		/// <summary>
		/// Default constructor
		/// </summary>
		public DownloadFile()
		{
		}
		/// <summary>
		/// Constructor using source folder path to set property values
		/// </summary>
		public DownloadFile(SourceFilePath sourceFilePath)
		{
			DestinationSubfolder = sourceFilePath.DestinationSubfolder;
			fileFolder = sourceFilePath.FolderPath;
		}
		#endregion

		#region IEquatable implementation and hash/equality overrides
		public bool Equals(DownloadFile other)
		{
			return ((object)other == null) ? false : (GetHashCode() == other.GetHashCode());
		}
		public override int GetHashCode() => HashCode.Combine(fileFolder, fileName, lastWriteTime, size);
		public override bool Equals(object obj) => obj is DownloadFile df && Equals(df);
		public static bool operator ==(DownloadFile df1, DownloadFile df2)
		{
			if ((object)df1 == null && (object)df2 == null)
			{
				return true;
			}
			else if ((object)df1 == null || (object)df2 == null)
			{
				return false;
			}
			else
			{
				return df1.Equals(df2);
			}
		}
		public static bool operator !=(DownloadFile df1, DownloadFile df2)
		{
			if ((object)df1 == null && (object)df2 == null)
			{
				return false;
			}
			else if ((object)df1 == null || (object)df2 == null)
			{
				return true;
			}
			else
			{
				return !df1.Equals(df2);
			}
		}
		#endregion
	}
}
