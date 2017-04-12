using Mh.Entries;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Mh.MongoRepository
{
    public class MongoRepositoryBase<TEntity,TKey> where TEntity:IEntity<TKey>
    {
        readonly string _dbName;
        readonly string _collName;
        IMongoClient _client;
        public MongoRepositoryBase(string connStr,string dbName,string collName=null)
        {
            _client = new MongoClient(connStr);
            _dbName = dbName;
            _collName = string.IsNullOrEmpty(collName) ? typeof(TEntity).Name : collName;
        }
        IMongoCollection<TEntity> Collection => _client.GetDatabase(_dbName).GetCollection<TEntity>(_collName);
        protected static FilterDefinitionBuilder<TEntity> Filter => new FilterDefinitionBuilder<TEntity>();
        protected static UpdateDefinitionBuilder<TEntity> Updater => new UpdateDefinitionBuilder<TEntity>();
        protected static SortDefinitionBuilder<TEntity> Sort => new SortDefinitionBuilder<TEntity>();
        protected static ProjectionDefinitionBuilder<TEntity> Projection => new ProjectionDefinitionBuilder<TEntity>();

        public  Task InsertOneAsync(TEntity entity)
        {
            return Collection.InsertOneAsync(entity);
        }

    }
}
