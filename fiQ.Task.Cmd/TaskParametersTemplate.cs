using System;
using System.Collections.Generic;
using System.IO;

namespace fiQ.Task.Cmd
{
	/// <summary>
	/// Intermediate class for reading a TaskParameters object from a configuration file
	/// </summary>
	internal class TaskParametersTemplate
	{
		#region Fields
		public string TaskName { get; init; }
		public string AdapterClassName { get; init; }
		public string AdapterDLLName { get; init; }
		public string AdapterDLLPath { get; init; }
		public Dictionary<string, string> Parameters { get; init; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		#endregion

		#region Methods
		public TaskParametersTemplate EnsureValid()
		{
			if (string.IsNullOrEmpty(AdapterClassName) || Parameters.Count == 0) throw new InvalidDataException("Required parameters missing");
			return this;
		}
		#endregion
	}
}
