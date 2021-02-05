using System;
using System.Collections.Generic;
using System.IO;

namespace fiQ.TaskUtilities
{
	/// <summary>
	/// Wrapper class to hold and handle disposal of a series of nested streams
	/// </summary>
	/// <remarks>
	/// Used for operations such as PGP encryption, where data must flow through multiple layers of processing
	/// </remarks>
	public class StreamStack : IDisposable
	{
		#region Fields
		private Stack<Stream> streams = new Stack<Stream>();
		private bool disposed = false;

		public bool Empty
		{
			get
			{
				return (streams.Count == 0);
			}
		}
		#endregion

		#region IDisposable implementation
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		protected virtual void Dispose(bool disposing)
		{
			if (disposed == false && disposing)
			{
				while (streams.Count > 0)
				{
					var stream = streams.Pop();
					stream.Dispose();
				}
			}
			disposed = true;
		}
		#endregion

		#region Methods
		/// <summary>
		/// Retrieve stream at top of stack
		/// </summary>
		public Stream GetStream()
		{
			return streams.Count > 0 ? streams.Peek() : null;
		}
		/// <summary>
		/// Add stream to top of stack
		/// </summary>
		internal void PushStream(Stream stream)
		{
			streams.Push(stream);
		}
		#endregion
	}
}
