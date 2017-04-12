using System;
using System.Collections.Generic;
using System.Text;

namespace Mh.Entries
{
    public interface IEntity<TKey>
    {
         TKey ID { get; set; }
    }
}
