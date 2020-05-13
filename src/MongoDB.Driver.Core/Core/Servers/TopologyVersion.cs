using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Servers
{
    /// <summary>
    /// 
    /// </summary>
    public class TopologyVersion : IConvertibleToBsonDocument
    {
        #region static
        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public static TopologyVersion Parse(BsonDocument document)
        {
            // TODO
            var processId = Ensure.IsNotNull(document.GetValue("processId", null), nameof(document)).AsObjectId;
            var counter = Ensure.IsNotNull(document.GetValue("counter", null), nameof(document)).AsInt64;

            return new TopologyVersion(processId, counter);
        }
        #endregion

        private readonly long _counter;
        private readonly ObjectId _processId;

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="processId">TODO</param>
        /// <param name="counter">TODO</param>
        public TopologyVersion(ObjectId processId, long counter)
        {
            _processId = processId;
            _counter = counter;
        }

        /// <summary>
        /// Gets the processId.
        /// </summary>
        public ObjectId ProcessId => _processId;

        /// <summary>
        /// Gets the counter;
        /// </summary>
        public long Counter => _counter;

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            //TODO
            return base.Equals(obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            // TODO
            return base.GetHashCode();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public BsonDocument ToBsonDocument()
        {
            return new BsonDocument
            {
                { "processId", _processId } ,
                { "counter", _counter }
            };
        }
    }
}
