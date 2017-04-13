using Mh.Entries;
using System;
using System.Collections.Generic;
using System.Text;

namespace Mh.MongoRepository.TestInstructure
{
    public class OrderRepositoryBase<TEntity, TKey> : MongoRepositoryBase<TEntity, TKey> where TEntity:class ,IEntity<TKey>,new ()
    {
        public OrderRepositoryBase() : base("mongodb://127.0.0.1", "OrderCenter")
        {
        }
    }
    public class OrderStringIdRepository:OrderRepositoryBase<OrderStringId,string>
    {
    }
    public class OrderObjectIdRepository : OrderRepositoryBase<OrderObjectId, string>
    {
    }
    public class OrderIncIdRepository : OrderRepositoryBase<OrderIncId, long>
    {
    }
}
