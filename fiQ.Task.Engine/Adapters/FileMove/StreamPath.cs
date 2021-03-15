using System;
using System.IO;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Wrapper class to hold (and handle disposal of) a Stream handle alongside a string providing full
	/// remote path of the file to which Stream points
	/// </summary>
	class StreamPath : IDisposable
	{
		#region Fields and properties
		private bool disposed = false;
		public Stream stream { get; init; }
		public string path { get; init; }
		#endregion

		#region IDisposable implementation
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		protected virtual void Dispose(bool disposing)
		{
			if (disposed == false && disposing && stream != null)
			{
				stream.Dispose();
			}
			disposed = true;
		}
		#endregion
	}
}
