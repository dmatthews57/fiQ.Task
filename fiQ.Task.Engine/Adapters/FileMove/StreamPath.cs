using System;
using System.IO;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Wrapper class to hold (and handle disposal of) a Stream handle alongside a string indicating path of stream
	/// </summary>
	class StreamPath : IDisposable
	{
		#region Fields and properties
		private bool disposed = false;
		public Stream stream { get; set; }
		public string path { get; set; }
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
				stream = null;
			}
			disposed = true;
		}
		#endregion
	}
}
