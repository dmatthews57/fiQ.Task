using System;
using System.Text.Json.Serialization;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Container class for information on a specific file that has been (or can be) downloaded by FileMoveAdapter
	/// </summary>
	internal class DownloadFile : IEquatable<DownloadFile>
	{
		#region Standard properties
		/// <summary>
		/// Source subfolder from which file was (or will be) downloaded
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
		#endregion

		#region Special properties
		/// <summary>
		/// Destination subfolder to which file will be downloaded (not serialized or included in hash)
		/// </summary>
		/// <remarks>
		/// Only relevant for files to be downloaded
		/// </remarks>
		[JsonIgnore(Condition = JsonIgnoreCondition.Always)]
		public string DestinationSubfolder { get; } = null;

		/// <summary>
		/// Date file was downloaded (serialized but not part of hash)
		/// </summary>
		/// <remarks>
		/// Only relevant for files that have already been downloaded
		/// </remarks>
		public DateTime downloadedAt { get; } = DateTime.Now;
		#endregion

		#region Constructors
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
