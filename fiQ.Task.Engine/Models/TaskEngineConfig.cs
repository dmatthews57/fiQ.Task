namespace fiQ.TaskModels
{
	/// <summary>
	/// Container class for TaskEngine configuration parameters
	/// </summary>
	public class TaskEngineConfig
	{
		/// <summary>
		/// Flag to indicate if batch execution should halt in case of single step failure (default false)
		/// </summary>
		public bool HaltOnError { get; set; } = false;
		/// <summary>
		/// Flag to trigger debug logging of all return values at end of batch
		/// </summary>
		public bool DebugReturnValues { get; set; } = false;
	}
}
