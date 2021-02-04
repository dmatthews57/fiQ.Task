using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace fiQ.TaskUtilities
{
	public static class Streams
	{
		/// <summary>
		/// Asynchronously calculate hash of all data in provided stream
		/// </summary>
		/// <param name="stream">Input stream containing data (caller is responsible for disposing)</param>
		/// <param name="hashprovider">Hash provider created by caller (who is responsible for disposing)</param>
		/// <remarks>
		/// Stream is assumed to be at Position 0 when received, and will be at ending position when returned; caller
		/// is responsible for resetting stream position after calling this function, if data must still be read
		/// </remarks>
		public static async Task<byte[]> GetStreamHash(Stream stream, IncrementalHash hashprovider)
		{
			var buffer = new byte[1024];
			while (stream.CanRead)
			{
				int br = await stream.ReadAsync(buffer, 0, 1000);
				if (br > 0)
				{
					hashprovider.AppendData(buffer, 0, br);
				}
				else break;
			}
			return hashprovider.GetHashAndReset();
		}
	}
}
