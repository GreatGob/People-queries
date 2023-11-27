// LAB 7

/*  
    Bài 1: Đếm tất cả những người có tên là Pauline Fournier.
    Kết quả trả về sẽ là: 67. 
*/
db.lab7.find(
    {
        "firstName": "Pauline", 
        "lastName": "Fournier"
    }
).count()


/*  
    Bài 2: Đếm tất cả những người có tên là Pauline Fournier 
    và sinh trước ngày 01/01/1970. 
    Kết quả trả về sẽ là: 9.
*/
db.lab7.find(
    {
        "firstName": "Pauline", 
        "lastName": "Fournier",
        "birthDate": {
            $lt: ISODate("1970-01-01T00:00:00.000Z")
        }
    }
).count()


/*  
    Bài 3: Đếm tất cả những người có tên:
    Lucas Dubois
    Camille Dubois
    Kết quả trả về sẽ là: 471. 
*/
db.lab7.find(
    {
        "firstName": {$in: ["Lucas", "Camille"]},
        "lastName": "Dubois"
    }
).count()


/*  
    Bài 4: Đếm tất cả những người không có khoản tín dụng (credits) nào. 
    Bạn có thể tìm thấy các khoản tín dụng trong trường wealth.credits.,
    trường này là một mảng, vì mọi người có thể có một hoặc nhiều khoản 
    tín dụng, nếu là mảng rỗng thì tức là không có khoản tín dụng nào.

    Kết quả trả về sẽ là: 83089. 
*/
db.lab7.find(
    {
        "wealth.credits": []
    }
).count()


/*  
    Bài 5: Đếm tất cả những người đã tiêu chính xác 12.99$ cho rạp chiếu 
    phim(cinema). Tất cả các khoản thanh toán được lưu trữ trong trường 
    mảng payments, bạn hãy xem qua cấu trúc các phần tử trong mảng này 
    để viết truy vấn cho hợp lý.
    
    Kết quả trả về sẽ là: 270. 
*/
db.lab7.find(
    {
        "payments": {
            $elemMatch: {
                "name": "cinema",
                "amount": 12.99
            }
        }
    }
).count()


/*  
    Bài 6: Hãy đếm tất cả những người có lần thanh toán đầu tiên là thanh
    toán 12.99$ cho rạp chiếu phim (cinema). Ở bài này bạn chỉ đếm các 
    trường hợp có payments[0] thỏa mãn yêu cầu trên.

    Kết quả trả về sẽ là: 24. 
*/
db.lab7.find({
    "payments.0.name": "cinema",
    "payments.0.amount": 12.99,
    "payments.0": { $exists: true }
}).count()


/*  
    Bài 7: Hãy đếm tất cả những người chưa bao giờ đến rạp chiếu phim 
    (chưa có khoản thanh toán nào dành cho cinema).

    Kết quả trả về sẽ là: 79996.
*/
db.lab7.find({
    "payments": {
        $not: {
            $elemMatch: {"name": "cinema"}
        }
    }
}).count()


/*  
    Bài 8: Hãy đếm tất cả những phụ nữ đã chi hơn 100$ cho giày (shoes) 
    và hơn 50$ cho quần (pants) trong 1 hóa đơn.

    Kết quả trả về sẽ là: 913.
*/
db.lab7.find({
    "sex": "female",
    "payments": {
        $all: [
            { $elemMatch: { name: "shoes", amount: { $gt: 100 } } },
            { $elemMatch: { name: "pants", amount: { $gt: 50 } } },
        ]
    }
}).count()

/*  
    Bài 9: Hãy đếm tất cả những người từ Warsaw, Poland đã đến rạp 
    chiếu phim (cinema) nhưng chưa bao giờ đến vũ trường (disco).

    Kết quả trả về sẽ là: 13352.
*/
db.lab7.find({
    "address.city": "Warsaw",
    "address.country": "Poland",
    $and: [
        { "payments.name": "cinema" },
        { "payments.name": { $ne: "disco" } }
    ]
}).count()

/*  
    Bài 10: Đếm tất cả phụ nữ từ Paris và đàn ông từ Cracow mà có tất cả các tài sản sau:
    flat
    house
    land
    Ít nhất một trong số các tài sản đó phải có giá trên 2.000.000$, và không tài sản nào trong số đó có giá dưới 500.000$.
    
    Gợi ý, các tài sản được lưu trữ ở trường "wealth.realEstates"

    Kết quả trả về sẽ là: 23.
*/
db.lab7.find({
    $and: [
        {
            $or: [
                { "sex": "male", "address.city": "Cracow"  },
                { "sex": "female", "address.city": "Paris"}
            ]
        },
        { "wealth.realEstates.type": "flat" },
        { "wealth.realEstates.type": "house" },
        { "wealth.realEstates.type": "land" },
        { "wealth.realEstates.worth": { $gt: 2000000 } },
        { "wealth.realEstates.worth": { $not: { $lt: 500000 } } },
    ]
}).count();


/*  
    Bài 11: Đếm tất cả những người có đúng 10 giao dịch.

    Kết quả trả về sẽ là: 179972. 
*/
db.lab7.find(
    {
        "payments": { $size: 10}
    }
).count()


/*  
    Bài 12: Tìm tất cả những người có firstName = 'Thomas' 
    và chỉ trả về các trường sau: _id, firstName và lastName. 
*/
db.lab7.find(
    {
        "firstName": "Thomas"
    },
    {
        "_id": 1, "firstName": 1, "lastName": 1
    }
)


/*  
    Bài 13: Tìm tất cả những người có một hoặc nhiều giao dịch có giá trị bé hơn 5$. 
    Kết quả trả về chỉ gồm các trường firstName, lastName và payments chỉ chứa phần 
    từ đầu tiên có amount bé hơn 5$. 
*/
db.lab7.find(
    {
        "payments": {
            $elemMatch: {
                "amount": { $lt: 5}
            }
        }
    },
    {
        "_id": 1, "firstName": 1, "lastName": 1, "payments.$": 1
    }
)


/*  
    Bài 14: Thêm một phần tử vào payments của những người đang ở Pháp (France) 
    với cấu trúc như sau:

    {
        category: "relax",
        name: "disco",
        amount: 5.06
    } 
*/
db.lab7.updateMany(
    {
        "address.country": "France"
    },
    {
        $push: {
            category: "relax",
            name: "disco",
            amount: 5.06
        }
    }
)
/* Return:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 101238,
  modifiedCount: 101238,
  upsertedCount: 0
} 
*/


/*  
    Bài 15: Xóa tất cả các trường market của tất cả mọi người 
*/
db.lab7.updateMany(
    {},
    {
        $unset: {
            "wealth.market": 1
        }

})
/*  Return:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 200000,
  modifiedCount: 200000,
  upsertedCount: 0
}
*/
