using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace fiQ.Task.Models
{
	[DataContract]
	class TaskAdapterConfig
	{
		#region Fields
		[DataMember]
		public string AdapterClassName { get; set; }
		[DataMember]
		public string AdapterDLLName { get; set; } = null;
		[DataMember]
		public string AdapterDLLPath { get; set; } = null;

		/// <summary>
		/// Private (non-scrubbed) version of parameter collection
		/// </summary>
		[DataMember]
		private Dictionary<string, string> TaskParameters { get; set; }
		#endregion

		#region Properties
		/// <summary>
		/// Retrieve key/value pairs from TaskParameters, transforming to pass to TaskAdapterConfig
		/// </summary>
		[IgnoreDataMember]
		public IReadOnlyDictionary<string, string> Parameters
		{
			get
			{
				return TaskParameters
					.Where(a => a.Key.StartsWith("@")) // Ignore keys not starting with "@"
					.ToDictionary(b => b.Key[1..], b => b.Value); // Strip leading "@" from key name
			}
		}
		#endregion
	}
}
