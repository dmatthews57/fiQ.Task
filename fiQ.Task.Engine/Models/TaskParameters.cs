using System;
using System.Collections.Generic;

namespace fiQ.Task.Models
{
	/// <summary>
	/// Container class for parameters to be passed as input to a TaskAdapter's ExecuteTask function
	/// </summary>
	public class TaskParameters
	{
		#region Fields
		private Dictionary<string, string> parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		#endregion
	}
}
