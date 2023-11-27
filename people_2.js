// LAB 8

/*  
    Bài 1: Đếm số người của từng quốc gia.
*/
db.lab8.aggregate([
    {
        $group: {
            _id: "$address.country",
            total: { $sum: 1 } 
        }
    }
]).pretty()

/* 
    Bài 2: Địa chỉ phổ biến nhất là gì và có bao nhiêu người sống ở đó?
*/
db.lab8.aggregate([
    {
        $group: {
            _id: { 
                country: "$address.country",
                city: "$address.city",
                postalCode: "$address.postalCode",
                street: "$address.street"
            },
            total: { $sum: { $toDouble: 1 } } 
        }
    }
]).pretty()

/*  
    Bài 3: Mỗi quốc gia có bao nhiêu người đã từng thanh toán ở một nhà hàng (restaurant)?
*/
db.lab8.aggregate([
    {
        $match: {
            "payments.name": "restaurant"
        }
    },
    {
        $group: {
            _id: "$address.country",
            visit: { $sum: { $toDouble: 1 } } 
        }
    }
]).pretty()

/*  
    Bài 4: Tìm 3 người có tổng số dư tài khoản nhiều nhất. Nếu như một người có 
    cùng tổng số dư tài sản thì hãy so sánh bằng trường "firstName" và "lastName"

    Gợi ý: Các số dư tài sản được lưu trữ trong trường "wealth.bankAccounts.balance"
*/
db.lab8.aggregate([
    {
        $project: {
            firstName: 1,
            lastName: 1,
            totalBalance: { $sum: "$wealth.bankAccounts.balance" }
        }
    },
    {
        $sort: { 
            totalBalance: -1,
            firstName: 1,
            lastName:1
        }
    },
    {
        $limit: 3
    }
]).pretty()

/*  
    Bài 5: Đếm số lần thanh toán ở nhà hàng, tổng số tiền đã chi tiêu và số 
    tiền trung bình cho mỗi lần thanh toán chia theo từng quốc gia. 
*/
db.lab8.aggregate([
    {
        $unwind: "$payments"
    },
    {
        $match: {
          "payments.name": "restaurant"
        }
    },
    {
        $group: {
            _id: "$address.country",
            totalVisits: { $sum: 1},
            totalAmount: { 
                $sum: {
                    $toDouble: "$payments.amount"
                } 
            },
            avgAmount: {
                $avg: {
                    $toDouble: "$payments.amount"
                }
            }
        }
    },
    {
        $project: {
            _id: 1,
            totalVisits: 1,
            totalAmount: 1,
            avgAmount: 1
        }
      }
]).pretty()

/*  
    Bài 6: Có một quốc gia mà mức thanh toán trung bình tại một nhà hàng là cao nhất 
    và một quốc gia trong đó mức thanh toán trung bình tại một nhà hàng thấp nhất. 
    Số người của nước thứ nhất chi tiêu nhiều hơn người ở nước thứ hai bao nhiêu lần?
*/
db.lab8.aggregate([
    {
        $unwind: "$payments"
    },
    {
        $match: {
            "payments.name": "restaurant"
        }
    },
    {
        $group: {
            _id: "$address.country",
            avgAmount: { $avg: "$payments.amount" }
        }
    },
    {
        $sort: { avgAmount: -1 }
    },
    {
        $group: {
            _id: null,
            maxAvgAmount: { $first: "$avgAmount" },
            minAvgAmount: { $last: "$avgAmount" }
        }

    },
    {
        $project: {
            _id: 1,
            diff: { $divide: ["$maxAvgAmount", "$minAvgAmount"]}
        }
    }
]).pretty()

/*  
    Bài 7: Viết truy vấn tìm tất cả những người có một hoặc nhiều giao dịch có giá trị bé hơn 5$. 
    Kết quả trả về chỉ gồm các trường firstName, lastName và mảng payments chứa TẤT CẢ phần tử có 
    amount bé hơn 5$.
*/
db.lab8.aggregate([
    {
        $unwind: "$payments"
    },
    {
          $match: {
            "payments.amount": { $lt: 5 }
          }
    },
    {
        $group: {
            _id: "$_id",
            firstName: { $first: "$firstName" },
            lastName: { $first: "$lastName" },
            payments: { $push: "$payments" }
        }
    },
    {
        $project: {
            _id: 1,
            firstName: 1,
            lastName: 1,
            payments: 1
        }
    }
]).pretty()

/*  
    Bài 8: Viết truy vấn để tính tổng giá trị mà một người đã thanh toán theo từng category. 
*/
db.lab8.aggregate([
    {
        $unwind: "$payments"
    },
    {
        $group: {
            _id: {
                _id: "$_id",
                firstName: "$firstName",
                lastName: "$lastName",
                category: "$payments.category"
            },
            totalAmount: { $sum: "$payments.amount" }
        }
    },
    {
        $group: {
            _id: {
                _id: "$_id._id",
                firstName: "$_id.firstName",
                lastName: "$_id.lastName"
            },
            totalPayments: {
                $push: {
                    category: "$_id.category",
                    amount: "$totalAmount"
                }
            }
        }
    },
    {
        $project: {
            _id: {
                $toString: "$_id._id"
              },
            firstName: "$_id.firstName",
            lastName: "$_id.lastName",
            totalPayments: 1
        }
    }
]).pretty()

/*  
    (Nâng cao) Bài 9:  Đếm số người ở mỗi quốc gia theo các nhóm tuổi như sau:

    18-29
    30-39
    40-49
    Trước hết, số lượng người trong mỗi nhóm phụ thuộc vào ngày hiện tại. Nếu thực hiện truy vấn của mình 
    hôm nay hoặc trong một tuần, chúng ta có thể nhận được các kết quả khác nhau. Điều này là do mọi người 
    đang già đi mỗi ngày. Vì vậy, điều rất quan trọng là đặt một ngày tùy ý sẽ được sử dụng để tính tuổi 
    của một người. Vậy nên chúng ta sẽ giả sử rằng ngày hiện tại là: 22/06/2016
*/
db.lab8.aggregate([
    {
        $addFields: {
            current_date: ISODate("2016-06-22T00:00:00Z")
        }
    },
    {
        $addFields: {
            age: {
                $floor: {
                    $divide: [{
                        $subtract: [
                            "$current_date", { $toDate:"$birthDate"}
                        ]
                    }, 31536000000]
                }
            }
        }
    },
    {
        $addFields: {
            ageRange: {
                $switch: {
                    branches: [
                        {
                            case: {
                                $and: [
                                    { $gte: [ "$age", 18 ] },
                                    { $lte: [ "$age", 29 ] }
                                ]
                            },
                            then: "18-29"
                        },
                        {
                            case: {
                                $and: [
                                    { $gte: [ "$age", 30 ] },
                                    { $lte: [ "$age", 39 ] }
                                ]
                            },
                            then: "30-39"
                        },
                        {
                            case: {
                                $and: [
                                    { $gte: [ "$age", 40 ] },
                                    { $lte: [ "$age", 49 ] }
                                ]
                            },
                            then: "40-49"
                        }
                    ],
                    default: "Other"
                }
            }
        }
    },
    {
        $match: {
            ageRange: { $ne: "Other" }
        }
    },
    {
        $group: {
            _id: {
                ageRange: "$ageRange",
                country: "$address.country"
            },
            count: { $sum: 1 },
        }
    },
    {
        $project: {
            _id: 0,
            ageRange: "$_id.ageRange",
            country: "$_id.country",
            count: 1
        }
    }
])

/*
    (Nâng cao) Bài 10:  Đếm số người ở mỗi quốc gia theo các nhóm tuổi như sau:

    Tính phần trăm dân số của cả nước thuộc các nhóm tuổi:

    18-29
    30-39
    40-49
    Kết quả phải được làm tròn đến hai chữ số thập phân.

*/
db.lab8.aggregate([
    {
        $addFields: {
            current_date: ISODate("2016-06-22T00:00:00Z")
        }
    },
    {
        $addFields: {
            age: {
                $floor: {
                    $divide: [{
                        $subtract: [
                            "$current_date", { $toDate:"$birthDate"}
                        ]
                    }, 31536000000]
                }
            }
        }
    },
    {
        $addFields: {
            ageRange: {
                $switch: {
                    branches: [
                        {
                            case: {
                                $and: [
                                    { $gte: [ "$age", 18 ] },
                                    { $lte: [ "$age", 29 ] }
                                ]
                            },
                            then: "18-29"
                        },
                        {
                            case: {
                                $and: [
                                    { $gte: [ "$age", 30 ] },
                                    { $lte: [ "$age", 39 ] }
                                ]
                            },
                            then: "30-39"
                        },
                        {
                            case: {
                                $and: [
                                    { $gte: [ "$age", 40 ] },
                                    { $lte: [ "$age", 49 ] }
                                ]
                            },
                            then: "40-49"
                        }
                    ],
                    default: "Other"
                }
            }
        }
    },
    {
        $match: {
            ageRange: { $ne: "Other" }
        }
    },
    {
        $group: {
            _id: {
                ageRange: "$ageRange",
                country: "$address.country"
            },
            count: { $sum: 1 }
        }
    },
    {
        $group: {
            _id: "$_id.country",
            ageRanges: {
                $push: {
                    ageRange: "$_id.ageRange",
                    count: "$count"
                }
            },
            totalPopulation: { $sum: "$count" }
        }
    },
    {
        $unwind: "$ageRanges"
    },
    {
        $project: {
            _id: 0,
            country: "$_id",
            ageRange: "$ageRanges.ageRange",
            percent: {
                $round: [
                    {
                        $multiply: [
                            { $divide: ["$ageRanges.count", "$totalPopulation"] },
                            100
                        ]
                    },
                    2
                ]
            }
        }
    }
]);

  

  