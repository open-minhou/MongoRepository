using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using Mh.MongoRepository.TestInstructure;

namespace Mh.MongoRepository.Test
{
    [TestClass]
    public class OrderStringIdTest
    {
        static OrderStringIdRepository _repository;
        static OrderStringId _order;
        static OrderStringIdTest()
        {
            _repository = new OrderStringIdRepository();
            _order = new OrderStringId { ID=Guid.NewGuid().ToString("N"),Name="testName"};
        }
        [TestMethod]
        public async Task InsertAndGetTest()
        {
            await _repository.InsertAsync(_order);
            var exist = _repository.Exists(a => a.ID == _order.ID);
            var result = await _repository.GetAsync(_order.ID);
            var result1 = await _repository.GetAsync(a => a.ID == _order.ID);
            Assert.AreEqual(_order.ID, result.ID);
            Assert.AreEqual(_order.ID, result1.ID);
            Assert.AreEqual(_order.Name, result.Name);
            Assert.AreEqual(exist, true);
            await _repository.DeleteOneAsync(a => a.ID == _order.ID);
            exist = _repository.Exists(a => a.ID == _order.ID);
            Assert.AreEqual(exist, false);
        }
    }
}
