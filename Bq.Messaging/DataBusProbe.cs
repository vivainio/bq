namespace Bq.Messaging.DataBus
{
    /// <summary>Lets tests reach the file share implementation directly.</summary>
    internal sealed class FileShareDataBusImplementationProbe
    {
        readonly string path;
        public FileShareDataBusImplementationProbe(string path) { this.path = path; }
        public IDataBus Create() => new FileShareDataBusImplementation(path);
    }
}
