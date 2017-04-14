using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using Mh.MongoRepository.TestInstructure;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Linq;

namespace Mh.MongoRepository.Test
{
    [TestClass]
    public class OrderStringIdTest
    {
        static OrderStringIdRepository _repository;
        static OrderStringIdTest()
        {
            _repository = new OrderStringIdRepository();
           
        }
        [TestMethod]
        public async Task InsertAndGetTest()
        {
            var _order = new OrderStringId { ID = Guid.NewGuid().ToString("N"), Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(_order);
            var exist =await _repository.ExistsAsync(a => a.ID == _order.ID);
            var result = await _repository.GetAsync(_order.ID);
            var result1 = await _repository.GetAsync(a => a.ID == _order.ID);
            Assert.AreEqual(_order.ID, result.ID);
            Assert.AreEqual(_order.ID, result1.ID);
            Assert.AreEqual(_order.Name, result.Name);
            Assert.AreEqual(exist, true);
            await _repository.DeleteOneAsync(a => a.ID == _order.ID);
            exist =await _repository.ExistsAsync(a => a.ID == _order.ID);
            Assert.AreEqual(exist, false);
        }
        [TestMethod]
        public async Task UpdateTest()
        {
            var _order = new OrderStringId { ID = Guid.NewGuid().ToString("N"), Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(_order,WriteConcern.Acknowledged);
            var exist = await _repository.ExistsAsync(a => a.ID == _order.ID);
            Assert.AreEqual(exist, true);
            var result = await _repository.GetAsync(_order.ID);
            Assert.AreEqual(_order.ID, result.ID);
            Assert.AreEqual(_order.Name, result.Name);
            _order.Name = Guid.NewGuid().ToString("N");
            await _repository.UpdateOneAsync(a => a.ID == _order.ID, _order);
            result = await _repository.GetAsync(_order.ID);
            Assert.AreEqual(_order.ID, result.ID);
            Assert.AreEqual(result.Name, _order.Name);
            await _repository.DeleteOneAsync(a => a.ID == _order.ID);
        }
        [TestMethod]
        public async Task DeleteByIDTest()
        {
            var _order = new OrderStringId { ID = Guid.NewGuid().ToString("N"), Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(_order);
            var result = await _repository.DeleteOneAsync(_order.ID);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.DeletedCount > 0);
        }
        [TestMethod]
        public async Task FindAndReplaceTest()
        {
            var _order = new OrderStringId { ID =Guid.NewGuid().ToString("N"), Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(_order);
            var newOrder= new OrderStringId { ID=_order.ID, Name = Guid.NewGuid().ToString("N") };
            var filter = new FilterDefinitionBuilder<OrderStringId>().Eq(nameof(OrderObjectId.ID), _order.ID);
            var result = await _repository.FindOneAndReplaceAsync(filter,newOrder,true);
            await _repository.DeleteManyAsync(filter);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.ID, _order.ID);
        }

        [TestMethod]
        public async Task FindAndReplaceLambadaTest()
        {
            var _order = new OrderStringId { ID = Guid.NewGuid().ToString("N"), Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(_order);
            var newOrder = new OrderStringId { ID = _order.ID, Name = Guid.NewGuid().ToString("N") };
            var result = await _repository.FindOneAndReplaceAsync(a=>a.ID== _order.ID, newOrder, true);
            await _repository.DeleteManyAsync(a => a.ID == _order.ID);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.ID, _order.ID);
        }
        [TestMethod]
        public async Task InsertBatchTest()
        {
            var list = new List<OrderStringId>();
            for (var i = 0; i < 10; i++)
            {
                var order = new OrderStringId { ID=Guid.NewGuid().ToString("N"),Name = Guid.NewGuid().ToString("N") };
                list.Add(order);
            }
            await _repository.InsertBatchAsync(list);
            var ids = list.Select(a => a.ID);
            var result = await _repository.GetListAsync(a => ids.Contains(a.ID));
            var filter = new FilterDefinitionBuilder<OrderStringId>().In(nameof(OrderStringId.ID), ids);
            var result2 = await _repository.GetListAsync(filter);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result2.Count > 0);
            await _repository.DeleteManyAsync( filter);
        }
        [TestMethod]
        public async Task UpdateOneTest()
        {
            var order = new OrderStringId { ID = Guid.NewGuid().ToString("N"), Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            order.Name = Guid.NewGuid().ToString("N");
            var result1 = await _repository.UpdateOneAsync(a => a.ID == order.ID, order);
            Assert.IsTrue(result1.MatchedCount == 1);
            Assert.IsTrue(result1.ModifiedCount == 1);
            order.Name = Guid.NewGuid().ToString("N");
            var filter1 = new FilterDefinitionBuilder<OrderStringId>().Eq(nameof(OrderStringId.ID), order.ID);
            var result2 = await _repository.UpdateOneAsync(filter1, order);
            Assert.IsTrue(result2.MatchedCount == 1);
            Assert.IsTrue(result2.ModifiedCount == 1);
            var updater1 = new UpdateDefinitionBuilder<OrderStringId>().Set(nameof(OrderStringId.Name), Guid.NewGuid().ToString("N"));
            var result3 = await _repository.UpdateOneAsync(filter1, updater1);
            Assert.IsTrue(result3.MatchedCount == 1);
            Assert.IsTrue(result3.ModifiedCount == 1);
            var updater2 = new UpdateDefinitionBuilder<OrderStringId>().Set(nameof(OrderStringId.Name), Guid.NewGuid().ToString("N"));
            var result4 = await _repository.UpdateOneAsync(a=>a.ID==order.ID, updater2);
            Assert.IsTrue(result4.MatchedCount == 1);
            Assert.IsTrue(result4.ModifiedCount == 1);
            var result5= await _repository.UpdateOneAsync(a => a.ID == order.ID, (bld)=> { return bld.Set(nameof(OrderStringId.Name), Guid.NewGuid().ToString("N")); });
            Assert.IsTrue(result5.MatchedCount == 1);
            Assert.IsTrue(result5.ModifiedCount == 1);
            await _repository.DeleteOneAsync(filter1);

        }
    }
}
