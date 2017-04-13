using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Mh.Entries
{
    public class Sequence : IEntity<string>
    {
        [BsonId]
        public string ID { get; set; }
        public long IncID { get; set; } 
    }
}
