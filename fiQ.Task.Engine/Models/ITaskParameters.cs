using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace fiQ.Task.Models
{
	/// <summary>
	/// Interface for configuration of a specific TaskAdapter to be executed
	/// </summary>
	public interface ITaskParameters
	{
		#region Fields and constructors
		public string TaskName { get; set; }
		public string AdapterClassName { get; set; }
		public string AdapterDLLName { get; set; }
		public string AdapterDLLPath { get; set; }
		#endregion

		#region Properties
		public IReadOnlyDictionary<string, string> Parameters { get; }
		#endregion

		#region Public methods - configuration accessors
		/// <summary>
		///  Retrieve count of parameters in configuration collection
		/// </summary>
		public int Count();

		/// <summary>
		/// Retrieve collection of dictionary keys (to allow independent iteration through parameters)
		/// </summary>
		public IEnumerable<string> GetKeys();

		/// <summary>
		/// Retrieve string value from parameter collection
		/// </summary>
		/// <param name="name">Name/dictionary key of parameter to retrieve</param>
		/// <param name="parmRegex">Optional Regex object to apply to parameter</param>
		/// <param name="processDateMacros">If set to true, date macros in value will be applied</param>
		/// <returns>Null if value does not match Regex, otherwise value (string.Empty if not found)</returns>
		public string GetString(string name, Regex parmRegex = null, bool processDateMacros = false);

		/// <summary>
		/// Retrieve boolean value from parameter collection
		/// </summary>
		/// <returns>True if value indicates, false otherwise (including if parameter not found)</returns>
		public bool GetBool(string name);
		#endregion

		#region Public methods - set parameter values
		/// <summary>
		/// Add (or update) a specific parameter name/value pair
		/// </summary>
		public void AddParameter(string name, string value);

		/// <summary>
		/// Merge a collection of values into parameter set (overwriting, if necessary)
		/// </summary>
		public void MergeParameters(IReadOnlyDictionary<string, string> values);
		#endregion
	}
}
