using Mh.Entries;
using Mh.MongoRepository.Enum;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Mh.MongoRepository
{
    /// <summary>
    /// MongoDB异步仓储
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <typeparam name="TKey">实体主键类型</typeparam>
    public class MongoRepositoryBase<TEntity, TKey> where TEntity : class,IEntity<TKey>,new()
    {
        /// <summary>
        /// 数据库名称
        /// </summary>
        readonly string _dbName;
        /// <summary>
        /// 集合名称（默认为实体类名）
        /// </summary>
        readonly string _collName;
        /// <summary>
        /// mongo连接
        /// </summary>
        IMongoClient _client;
        /// <summary>
        /// 创建一个仓储
        /// </summary>
        /// <param name="connStr">mongo连接字符串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collName">集合名称（默认实体类名）</param>
        public MongoRepositoryBase(string connStr, string dbName, string collName = null)
        {
            _client = new MongoClient(connStr);
            _dbName = dbName;
            _collName = string.IsNullOrEmpty(collName) ? typeof(TEntity).Name : collName;
        }
        protected static FilterDefinitionBuilder<TEntity> Filter => new FilterDefinitionBuilder<TEntity>();
        protected static UpdateDefinitionBuilder<TEntity> Updater => new UpdateDefinitionBuilder<TEntity>();
        protected static SortDefinitionBuilder<TEntity> Sort => new SortDefinitionBuilder<TEntity>();
        protected static ProjectionDefinitionBuilder<TEntity> Projection => new ProjectionDefinitionBuilder<TEntity>();

        private IMongoCollection<TEntity> GetCollection(WriteConcern writeConcern = null) =>
            writeConcern == null ? _client.GetDatabase(_dbName).GetCollection<TEntity>(_collName)
            : _client.GetDatabase(_dbName).GetCollection<TEntity>(_collName, new MongoCollectionSettings { WriteConcern = writeConcern });


        public async Task<UpdateDefinition<TEntity>> CreateUpdateDefinitionAsync(TEntity updateEntity, bool isUpsert = false)
        {
            UpdateDefinition<TEntity> updater;
            BsonDocument bsDoc = BsonExtensionMethods.ToBsonDocument(updateEntity);
            bsDoc.Remove("_id");
            updater = new BsonDocumentUpdateDefinition<TEntity>(bsDoc);
            if (isUpsert && updateEntity is IAutoInc)
            {
                long id = await GetIncID();
                updater = UpdateDefinitionExtensions.SetOnInsert(updater, "_id", id);
            }
            return updater;
        }

        public Task<DeleteResult> DeleteManyAsync(FilterDefinition<TEntity> filter, WriteConcern writeConcern = null)
        {
            return GetCollection(writeConcern).DeleteManyAsync(filter);
        }

        public Task<DeleteResult> DeleteManyAsync(Expression<Func<TEntity, bool>> filterExp, WriteConcern writeConcern = null)
        {
            return GetCollection(writeConcern).DeleteManyAsync(filterExp);
        }

        public Task<DeleteResult> DeleteOneAsync(Expression<Func<TEntity, bool>> filterExp, WriteConcern writeConcern = null)
        {
            return GetCollection(writeConcern).DeleteOneAsync(filterExp);
        }

        public Task<DeleteResult> DeleteOneAsync(FilterDefinition<TEntity> filter, WriteConcern writeConcern = null)
        {
            return GetCollection(writeConcern).DeleteOneAsync(filter);
        }

        public Task<DeleteResult> DeleteOneAsync(TKey id, WriteConcern writeConcern = null)
        {
            var filter = Filter.Eq("ID", id);
            return GetCollection(writeConcern).DeleteOneAsync(filter);
        }

        public Task<TEntity> FindOneAndDeleteAsync(FilterDefinition<TEntity> filter, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndDeleteOptions<TEntity>()
            {
                Sort = sort
            };
            return GetCollection(writeConcern).FindOneAndDeleteAsync(filter, options);
        }

        public Task<TEntity> FindOneAndDeleteAsync(Expression<Func<TEntity, bool>> filterExp, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, WriteConcern writeConcern = null)
        {

            var options = new FindOneAndDeleteOptions<TEntity>()
            {
                Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
            };
            return GetCollection(writeConcern).FindOneAndDeleteAsync(filterExp, options);
        }

        public Task<TEntity> FindOneAndReplaceAsync(FilterDefinition<TEntity> filter, TEntity entity, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndReplaceOptions<TEntity, TEntity>()
            {
                Sort = sort,
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).FindOneAndReplaceAsync(filter, entity, options);
        }

        public Task<TEntity> FindOneAndReplaceAsync(Expression<Func<TEntity, bool>> filterExp, TEntity entity, bool isUpsert = false, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndReplaceOptions<TEntity, TEntity>()
            {
                Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).FindOneAndReplaceAsync(filterExp, entity, options);
        }

        public Task<TEntity> FindOneAndUpdateAsync(FilterDefinition<TEntity> filter, UpdateDefinition<TEntity> update, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sort,// sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).FindOneAndUpdateAsync(filter, update, options);
        }


        public async Task<TEntity> FindOneAndUpdateAsync(FilterDefinition<TEntity> filter, TEntity updateEntity, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sort,// sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
                IsUpsert = isUpsert
            };
            var update = await CreateUpdateDefinitionAsync(updateEntity);
            return await GetCollection(writeConcern).FindOneAndUpdateAsync(filter, update, options);
        }

        public Task<TEntity> FindOneAndUpdateAsync(Expression<Func<TEntity, bool>> filterExp, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExp, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sort,// sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).FindOneAndUpdateAsync(filterExp, updateExp(Updater), options);
        }

        public Task<TEntity> FindOneAndUpdateAsync(Expression<Func<TEntity, bool>> filterExp, UpdateDefinition<TEntity> update, bool isUpsert = false, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).FindOneAndUpdateAsync(filterExp, update, options);
        }

        public async Task<TEntity> FindOneAndUpdateAsync(Expression<Func<TEntity, bool>> filterExp, TEntity updateEntity, bool isUpsert = false, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp),
                IsUpsert = isUpsert
            };
            var update = await CreateUpdateDefinitionAsync(updateEntity);
            return await GetCollection(writeConcern).FindOneAndUpdateAsync(filterExp, update, options);
        }

        public async Task InsertAsync(TEntity entity, WriteConcern writeConcern = null)
        {
            if (entity is IAutoInc incEntity)
            {
                incEntity.ID = await GetIncID();
                await GetCollection(writeConcern).InsertOneAsync((TEntity)incEntity);
                return;
            }
            await GetCollection(writeConcern).InsertOneAsync(entity);
        }

        public async Task InsertBatchAsync(IEnumerable<TEntity> entitys, WriteConcern writeConcern = null)
        {
            if (!(typeof(TEntity) is IAutoInc))
            {
                await GetCollection(writeConcern).InsertManyAsync(entitys);
                return;
            }
            var count = entitys.Count();
            var id = await GetIncID(count);
            List<TEntity> list = new List<TEntity>();
            foreach (var entity in entitys)
            {
                if (entity is IAutoInc incEntity)
                {
                    incEntity.ID = id - count + 1;
                    list.Add((TEntity)incEntity);
                    id--;
                }
            }
            if (list.Count > 0)
                await GetCollection(writeConcern).InsertManyAsync(list);
        }

        public Task<UpdateResult> UpdateManyAsync(FilterDefinition<TEntity> filter, UpdateDefinition<TEntity> update, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).UpdateManyAsync(filter, update, options);
        }

        public Task<UpdateResult> UpdateManyAsync(Expression<Func<TEntity, bool>> filterExp, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExp, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).UpdateManyAsync(filterExp, updateExp(Updater), options);
        }

        public Task<UpdateResult> UpdateManyAsync(Expression<Func<TEntity, bool>> filterExp, UpdateDefinition<TEntity> update, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).UpdateManyAsync(filterExp, update, options);
        }

        public Task<UpdateResult> UpdateOneAsync(FilterDefinition<TEntity> filter, UpdateDefinition<TEntity> update, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).UpdateOneAsync(filter, update, options);
        }

        public Task<UpdateResult> UpdateOneAsync(Expression<Func<TEntity, bool>> filterExp, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExp, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).UpdateOneAsync(filterExp, updateExp(Updater), options);
        }

        public Task<UpdateResult> UpdateOneAsync(Expression<Func<TEntity, bool>> filterExp, UpdateDefinition<TEntity> update, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).UpdateOneAsync(filterExp, update, options);
        }

        public async Task<UpdateResult> UpdateOneAsync(FilterDefinition<TEntity> filter, TEntity updateEntity, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            var update = await CreateUpdateDefinitionAsync(updateEntity);
            return await GetCollection(writeConcern).UpdateOneAsync(filter, update, options);
        }

        public async Task<UpdateResult> UpdateOneAsync(Expression<Func<TEntity, bool>> filterExp, TEntity updateEntity, bool isUpsert = false, WriteConcern writeConcern = null)
        {
            var options = new UpdateOptions
            {
                IsUpsert = isUpsert
            };
            var update = await CreateUpdateDefinitionAsync(updateEntity);
            return await GetCollection(writeConcern).UpdateOneAsync(filterExp, update, options);
        }

        async Task<long> GetIncID(int count = 1)
        {
            var updater = new UpdateDefinitionBuilder<Sequence>().Inc(nameof(Sequence.IncID), count);
            var setting = new MongoCollectionSettings { WriteConcern = WriteConcern.Acknowledged };
            var option = new FindOneAndUpdateOptions<Sequence>();
            option.IsUpsert = true;
            var filter = new FilterDefinitionBuilder<Sequence>().Eq("ID", _collName);
            var sequence = await _client.GetDatabase(_dbName).GetCollection<Sequence>("_Sequence", setting).FindOneAndUpdateAsync(filter, updater, option);
            return sequence.IncID;
        }

    }
}
