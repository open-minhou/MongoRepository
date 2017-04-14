using Mh.Entries;
using Mh.MongoRepository.Enum;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Mh.MongoRepository
{
    /// <summary>
    /// MongoDB异步仓储
    /// </summary>
    /// <typeparam name="TEntity">实体类型</typeparam>
    /// <typeparam name="TKey">实体主键类型</typeparam>
    public class MongoRepositoryBase<TEntity, TKey> where TEntity : class, IEntity<TKey>, new() 
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
        private IMongoCollection<TEntity> GetCollection(ReadPreference readConcern = null) =>
            readConcern == null ? _client.GetDatabase(_dbName).GetCollection<TEntity>(_collName)
            : _client.GetDatabase(_dbName).GetCollection<TEntity>(_collName, new MongoCollectionSettings { ReadPreference = readConcern });

        public async Task<UpdateDefinition<TEntity>> CreateUpdateDefinitionAsync(TEntity updateEntity, bool isUpsert = false)
        {
            UpdateDefinition<TEntity> updater;
            BsonDocument bsDoc = BsonExtensionMethods.ToBsonDocument(updateEntity);
            bsDoc.Remove("_id");
            updater = new BsonDocument("$set",bsDoc);
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

            var options = new FindOneAndDeleteOptions<TEntity>();
            if(sortExp!=null)
            {
                options.Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp);
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
                IsUpsert = isUpsert
            };
            if (sortExp != null)
            {
                options.Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp);
            }
            return GetCollection(writeConcern).FindOneAndReplaceAsync(filterExp, entity, options);
        }

        public Task<TEntity> FindOneAndUpdateAsync(FilterDefinition<TEntity> filter, UpdateDefinition<TEntity> update, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sort,
                IsUpsert = isUpsert,
            };
            return GetCollection(writeConcern).FindOneAndUpdateAsync(filter, update, options);
        }


        public async Task<TEntity> FindOneAndUpdateAsync(FilterDefinition<TEntity> filter, TEntity updateEntity, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sort,
                IsUpsert = isUpsert
            };
            var update = await CreateUpdateDefinitionAsync(updateEntity);
            return await GetCollection(writeConcern).FindOneAndUpdateAsync(filter, update, options);
        }

        public Task<TEntity> FindOneAndUpdateAsync(Expression<Func<TEntity, bool>> filterExp, Func<UpdateDefinitionBuilder<TEntity>, UpdateDefinition<TEntity>> updateExp, bool isUpsert = false, SortDefinition<TEntity> sort = null, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                Sort = sort,
                IsUpsert = isUpsert
            };
            return GetCollection(writeConcern).FindOneAndUpdateAsync(filterExp, updateExp(Updater), options);
        }

        public Task<TEntity> FindOneAndUpdateAsync(Expression<Func<TEntity, bool>> filterExp, UpdateDefinition<TEntity> update, bool isUpsert = false, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                IsUpsert = isUpsert
            };
            if (sortExp != null)
            {
                options.Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp);
            }
            return GetCollection(writeConcern).FindOneAndUpdateAsync(filterExp, update, options);
        }

        public async Task<TEntity> FindOneAndUpdateAsync(Expression<Func<TEntity, bool>> filterExp, TEntity updateEntity, bool isUpsert = false, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, WriteConcern writeConcern = null)
        {
            var options = new FindOneAndUpdateOptions<TEntity, TEntity>()
            {
                IsUpsert = isUpsert
            };
            if (sortExp != null)
            {
                options.Sort = sortType == SortType.Ascending ? Sort.Ascending(sortExp) : Sort.Descending(sortExp);
            }
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
            if (!(typeof(IAutoInc).IsAssignableFrom(typeof(TEntity))))
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
                    id++;
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

        public Task<List<TProjection>> AggregateAsync<TProjection>(Expression<Func<TEntity, bool>> filterExp, ProjectionDefinition<TEntity, TProjection> group, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = SortType.Ascending, int limit = 0, int skip = 0, ReadPreference readPreference = null)
        {
            FilterDefinition<TEntity> filter = null;
            if (filterExp != null)
            {
                filter = Filter.Where(filterExp);
            }
            else
            {
                filter = Filter.Empty;
            }
            IAggregateFluent<TProjection> fluent2 = CreateAggregate(filterExp, CreateSortDefinition(sortExp, sortType), readPreference).Group(group);
            if (skip > 0)
            {
                fluent2 = fluent2.Skip(skip);
            }
            if (limit > 0)
            {
                fluent2 = fluent2.Limit(limit);
            }
            return IAsyncCursorSourceExtensions.ToListAsync(fluent2);
        }
        public List<TResult> Aggregate<TResult, TID>(FilterDefinition<TEntity> filter, Expression<Func<TEntity, TID>> id, Expression<Func<IGrouping<TID, TEntity>, TResult>> group, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = 0, int limit = 0, int skip = 0, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            IAggregateFluent<TResult> fluent2 = IAggregateFluentExtensions.Group(CreateAggregate(filter, CreateSortDefinition(sortExp, sortType), readPreference), id, group);
            if (skip > 0)
            {
                fluent2 = fluent2.Skip(skip);
            }
            if (limit > 0)
            {
                fluent2 = fluent2.Limit(limit);
            }
            return IAsyncCursorSourceExtensions.ToList(fluent2);
        }
        public List<TResult> Aggregate<TResult, TID>(Expression<Func<TEntity, bool>> filterExp, Expression<Func<TEntity, TID>> id, Expression<Func<IGrouping<TID, TEntity>, TResult>> group, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = 0, int limit = 0, int skip = 0, ReadPreference readPreference = null)
        {
            FilterDefinition<TEntity> filter = null;
            if (filterExp != null)
            {
                filter = Filter.Where(filterExp);
            }
            else
            {
                filter = Filter.Empty;
            }
            return this.Aggregate<TResult, TID>(filter, id, group, sortExp, sortType, limit, skip, readPreference);
        }
        public Task<long> CountAsync(FilterDefinition<TEntity> filter, int limit = 0, int skip = 0, BsonValue hint = null, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            CountOptions options = CreateCountOptions(limit, skip, hint);
            return GetCollection(readPreference).CountAsync(filter, options);
        }
        public Task<long> CountAsync(Expression<Func<TEntity, bool>> filterExp, int limit = 0, int skip = 0, BsonValue hint = null, ReadPreference readPreference = null)
        {
            var filter = GetFIlter(filterExp);
            return CountAsync(filter, limit, skip, hint, readPreference);
        }
        public List<TField> Distinct<TField>(FieldDefinition<TEntity, TField> field, FilterDefinition<TEntity> filter, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            return IAsyncCursorExtensions.ToList(GetCollection(readPreference).Distinct(field, filter, null));
        }
        public List<TField> Distinct<TField>(Expression<Func<TEntity, TField>> fieldExp, FilterDefinition<TEntity> filter, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            return IAsyncCursorExtensions.ToList<TField>(IMongoCollectionExtensions.Distinct<TEntity, TField>(GetCollection(readPreference), fieldExp, filter, null));
        }
        public List<TField> Distinct<TField>(Expression<Func<TEntity, TField>> fieldExp, Expression<Func<TEntity, bool>> filterExp, ReadPreference readPreference = null)
        {
            FilterDefinition<TEntity> filter = GetFIlter(filterExp);
            return this.Distinct<TField>(fieldExp, filter, null);
        }
        public async Task<bool> ExistsAsync(FilterDefinition<TEntity> filter, BsonValue hint = null, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            var option=CreateCountOptions(1, 0, hint);
            return  await CountAsync(filter, 1, 0, hint, readPreference) > 0;
        }
        public Task<bool> ExistsAsync(Expression<Func<TEntity, bool>> filterExp, BsonValue hint = null, ReadPreference readPreference = null)
        {
            FilterDefinition<TEntity> filter = GetFIlter(filterExp);
            return this.ExistsAsync(filter, hint, readPreference);
        }
        public TEntity Get(FilterDefinition<TEntity> filter, ProjectionDefinition<TEntity, TEntity> projection = null, SortDefinition<TEntity> sort = null, BsonValue hint = null, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            FindOptions<TEntity, TEntity> options = CreateFindOptions(projection, sort, 1, 0, hint);
            return IAsyncCursorExtensions.FirstOrDefault(GetCollection(readPreference).FindSync(filter, options));
        }
        public async Task<TEntity> GetAsync(FilterDefinition<TEntity> filter, ProjectionDefinition<TEntity, TEntity> projection = null, SortDefinition<TEntity> sort = null, BsonValue hint = null, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            FindOptions<TEntity,TEntity> options = CreateFindOptions(projection, sort, 1, 0, hint);
            return await IAsyncCursorExtensions.FirstOrDefaultAsync(await GetCollection(readPreference).FindAsync(filter, options));
        }
        public  Task<TEntity> GetAsync(Expression<Func<TEntity, bool>> filterExp, Expression<Func<TEntity, TEntity>> includeFieldExp = null, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = 0, BsonValue hint = null, ReadPreference readPreference = null)
        {
            var filter = GetFIlter(filterExp);
            ProjectionDefinition<TEntity, TEntity> projection = null;
            if (includeFieldExp != null)
            {
                projection = IncludeFields(includeFieldExp);
            }
            var sort = CreateSortDefinition(sortExp, sortType);
            return GetAsync(filter, projection, sort, hint, readPreference);
        }
        public Task<TEntity> GetAsync(TKey id, Expression<Func<TEntity, TEntity>> includeFieldExp = null, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = 0, BsonValue hint = null, ReadPreference readPreference = null)
        {
            var filter = Filter.Eq("ID", id);
            ProjectionDefinition<TEntity, TEntity> projection = null;
            if (includeFieldExp != null)
            {
                projection = IncludeFields(includeFieldExp);
            }
            var sort = CreateSortDefinition(sortExp, sortType);
            return GetAsync(filter, projection, sort, hint, readPreference);
        }
       public async Task<List<TEntity>> GetListAsync(FilterDefinition<TEntity> filter, ProjectionDefinition<TEntity, TEntity> projection = null, SortDefinition<TEntity> sort = null, int limit = 0, int skip = 0, BsonValue hint = null, ReadPreference readPreference = null)
        {
            if (filter == null)
            {
                filter = Filter.Empty;
            }
            FindOptions<TEntity, TEntity> options = CreateFindOptions(projection, sort, limit, skip, hint);
            return await IAsyncCursorExtensions.ToListAsync(await GetCollection(readPreference).FindAsync(filter, options));
        }
        public Task<List<TEntity>> GetListAsync(Expression<Func<TEntity, bool>> filterExp = null, Expression<Func<TEntity, TEntity>> includeFieldExp = null, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = 0, int limit = 0, int skip = 0, BsonValue hint = null, ReadPreference readPreference = null)
        {
            FilterDefinition<TEntity> definition = GetFIlter(filterExp);
            ProjectionDefinition<TEntity, TEntity> projection = null;
            SortDefinition<TEntity> sort = null;
            sort = CreateSortDefinition(sortExp, sortType);
            if (includeFieldExp != null)
            {
                projection = IncludeFields(includeFieldExp);
            }
            return GetListAsync(definition, projection, sort, limit, skip, hint, readPreference);
        }

        async Task<long> GetIncID(int count = 1)
        {
            var updater = new UpdateDefinitionBuilder<Sequence>().Inc(nameof(Sequence.IncID), count);
            var setting = new MongoCollectionSettings { WriteConcern = WriteConcern.Acknowledged };
            var option = new FindOneAndUpdateOptions<Sequence>();
            option.IsUpsert = true;
            option.ReturnDocument = ReturnDocument.After;
            var filter = new FilterDefinitionBuilder<Sequence>().Eq("ID", _collName);
            var sequence = await _client.GetDatabase(_dbName).GetCollection<Sequence>("_Sequence", setting).FindOneAndUpdateAsync(filter, updater, option);
            return sequence.IncID;
        }
        protected IAggregateFluent<TEntity> CreateAggregate(FilterDefinition<TEntity> filter, SortDefinition<TEntity> sort, ReadPreference readPreference = null)
        {
            IAggregateFluent<TEntity> fluent = IMongoCollectionExtensions.Aggregate(this.GetCollection(readPreference), null).Match(filter);
            if (sort != null)
            {
                fluent = fluent.Sort(sort);
            }
            return fluent;
        }
        public CountOptions CreateCountOptions(int limit = 0, int skip = 0, BsonValue hint = null)
        {
            CountOptions options = new CountOptions();
            if (limit > 0)
            {
                options.Limit = limit;
            }
            if (skip > 0)
            {
                options.Skip = skip;
            }
            if (hint != null)
            {
                options.Hint = hint;
            }
            return options;
        }
        public FindOptions<TEntity, TEntity> CreateFindOptions(ProjectionDefinition<TEntity, TEntity> projection = null, SortDefinition<TEntity> sort = null, int limit = 0, int skip = 0, BsonValue hint = null)
        {
            FindOptions<TEntity, TEntity> options = new FindOptions<TEntity, TEntity>();
            if (limit > 0)
            {
                options.Limit = limit;
            }
            if (skip > 0)
            {
                options.Skip = skip;
            }
            if (projection != null)
            {
                options.Projection = projection;
            }
            if (sort != null)
            {
                options.Sort = sort;
            }
            if (hint != null)
            {
                BsonDocument document = new BsonDocument();
                document.Add("$hint", hint);
                options.Modifiers = document;
            }
            return options;
        }
        public FindOptions<TEntity, TEntity> CreateFindOptions(ProjectionDefinition<TEntity, TEntity> projection = null, Expression<Func<TEntity, object>> sortExp = null, SortType sortType = 0, int limit = 0, int skip = 0, BsonValue hint = null)
        {
            SortDefinition<TEntity> sort = CreateSortDefinition(sortExp, sortType);
            return CreateFindOptions(projection, sort, limit, skip, hint);
        }
        public SortDefinition<TEntity> CreateSortDefinition(Expression<Func<TEntity, object>> sortExp, SortType sortType = 0)
        {
            SortDefinition<TEntity> definition = null;
            if (sortExp == null)
            {
                return definition;
            }
            if (sortType == SortType.Ascending)
            {
                return Sort.Ascending(sortExp);
            }
            return Sort.Descending(sortExp);
        }
        public ProjectionDefinition<TEntity> IncludeFields(Expression<Func<TEntity, TEntity>> fieldsExp)
        {
            if (fieldsExp == null)
            {
                return null;
            }
            List<ProjectionDefinition<TEntity>> list = new List<ProjectionDefinition<TEntity>>();
            NewExpression body = fieldsExp.Body as NewExpression;
            if ((body == null) || (body.Members == null))
            {
                throw new Exception("fieldsExp is invalid expression format， eg: x => new { x.Field1, x.Field2 }");
            }
            foreach (MemberInfo info in body.Members)
            {
                list.Add(Projection.Include((FieldDefinition<TEntity>)info.Name));
            }
            return Projection.Combine(list);
        }

        FilterDefinition<TEntity> GetFIlter(Expression<Func<TEntity, bool>> filterExp)
        {
            FilterDefinition<TEntity> filter = null;
            if (filterExp != null)
            {
                filter = Filter.Where(filterExp);
            }
            else
            {
                filter = Filter.Empty;
            }
            return filter;
        }

    }
}
