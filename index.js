const express = require('express')

const cors = require('cors');

require("dotenv").config()

console.log(process.env.PORT, " PORTT");

const app = express()
const port = process.env.PORT || 6565

let AWS = require('aws-sdk');


let s3;

const configure = async () => {
    //configuring the AWS environment
    AWS.config.update({
        accessKeyId: process.env.AWS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_KEY,
        region: 'ap-south-1'
    });
    console.log("Region: ", AWS.config.region);

    s3 = new AWS.S3();



    // // // Set the Region 
    // // AWS.config.update({ region: 'ap-south-1' });
    // console.log("Region: ", AWS.config.region);

}

// {
//     Key: 'wheather_28_01_23_11:35:25.json',
//     LastModified: 2023-01-28T06:05:32.000Z,
//     ETag: '"28614aa89206fcbc1d38d8a2e834a19f"',
//     ChecksumAlgorithm: [],
//     Size: 156,
//     StorageClass: 'STANDARD',
//     Owner: [Object]
//   },
const getFilesList = async () => {

    //configuring parameters
    const params = {
        Bucket: 'surya-source-s3',
        Prefix: "wheather_",
        'MaxKeys': 5,
        // KeyCount: 2
        // // Body: fs.createReadStream(filePath),
        // Key: "folder/" + Date.now() + "_" + path.basename(filePath)
    };

    // let latestFile = null;

    console.log(" in get files list function");

    await s3.listObjects(params, async function (err, data) {
        if (err) {

            console.log(err, err.stack); // an error occurred
            resetData()
        } else {
            console.log(data, " datatta in bucket");

            data.Contents.sort(function (a, b) {
                return (b.LastModified > a.LastModified) ? 1 :
                    ((a.LastModified > b.LastModified) ? -1 : 0);
            });

            for (var file of data.Contents) {
                if (file.Key.endsWith('.json')) {
                    //extractData(file.Key);
                    console.log(file.Key, " file");
                    filesList = [file];
                    latestKey = file.Key;
                    break;

                }
            }
        }

    })

}

const getFileData = async () => {
    try {

        if (latestKey) {
            const params2 = {
                Bucket: 'surya-source-s3',
                Key: latestKey
            };
            await s3.getObject(params2
                , (err, data) => {
                    if (err) {
                        console.log("errorr ", err);
                        resetData()
                    } else {
                        // console.log("data.Body", data.Body);

                        const dataa = JSON.parse((data.Body).toString());

                        finalData = dataa;

                        console.log(dataa, " Data object");
                        // return dataa;
                    }

                })
        } else {
            console.log("No latest file found");

            resetData()
        }

        // return latestFile;
    } catch (error) {
        resetData()
    }



}


const getData = async () => {
    const key = "wheather_19_01_23_08:39:32.json"

    const params3 = {
        Bucket: 'surya-source-s3',
        Key: key
    };

    const data = await s3.getObject(params3);

    console.log(data.response, " Data in console");

    // s3.getObject(params2
    //     , (err, data) => {
    //         if (err) {
    //             console.log("errorr ", err);
    //         }

    //         console.log("data.Body", data.Body);

    //         const dataa = JSON.parse((data.Body).toString());

    //         console.log(dataa, " Data object");
    //         return dataa;
    //     })
}

const resetData = () => {
    finalData = null;
    filesList = [];
    latestKey = null;
}















































// /

setTimeout(async () => {
    // await getDataByQuery()

    // await getFilesListTest()
}, 2000);

const getFilesListTest = async () => {

    const query = 'SELECT * FROM s3object r;';
    //configuring parameters
    const params = {
        Bucket: 'surya-source-s3',
        Prefix: "wheather_",
        'MaxKeys': 5,
        // ExpressionType: 'SQL',
        // Expression: query,
        // Query: query
        // KeyCount: 2
        // // Body: fs.createReadStream(filePath),
        // Key: "folder/" + Date.now() + "_" + path.basename(filePath)
    };

    // let latestFile = null;

    console.log(" in get files list function");

    const dadad = await s3.listObjects(params).promise()

    console.log(dadad, " adadad");

    // await s3.listObjects(params, async function (err, data) {
    //     if (err) {

    //         console.log(err, err.stack); // an error occurred
    //         resetData()
    //     } else {
    //         console.log(data, " datatta in bucket");

    //         data.Contents.sort(function (a, b) {
    //             return (b.LastModified > a.LastModified) ? 1 :
    //                 ((a.LastModified > b.LastModified) ? -1 : 0);
    //         });

    //         for (var file of data.Contents) {
    //             if (file.Key.endsWith('.json')) {
    //                 //extractData(file.Key);
    //                 console.log(file.Key, " file");
    //                 filesList = [file];
    //                 latestKey = file.Key;
    //                 break;

    //             }
    //         }
    //     }

    // })

}

// {
//     Key: 'wheather_28_01_23_11:35:25.json',
//     LastModified: 2023-01-28T06:05:32.000Z,
//     ETag: '"28614aa89206fcbc1d38d8a2e834a19f"',
//     ChecksumAlgorithm: [],
//     Size: 156,
//     StorageClass: 'STANDARD',
//     Owner: [Object]
//   },

const getDataByQuery = async () => {
    try {
        // 1
        // const query = 'SELECT * FROM s3object[*].results[*] r;';
        const query = 'SELECT r.* FROM s3object r;';
        // 2
        const bucket = 'surya-source-s3';
        // const key = 'planets.json';
        const key = "wheather_19_01_23_08:39:32.json"

        // 3
        const params = {
            Bucket: bucket,
            //   Prefix: "wheather_",
            Key: key,
            ExpressionType: 'SQL',
            Expression: query,
            InputSerialization: {
                JSON: {
                    Type: 'DOCUMENT',
                }
            },
            OutputSerialization: {
                JSON: {
                    RecordDelimiter: ','
                }
            }
        }

        // 4
        const data = await getDataUsingS3Select(params);

        console.log(JSON.stringify(data), " datatatt");

        // context.succeed(data);
    } catch (error) {
        // context.fail(error);

        console.log(error, " errooror");
    }
}

const getDataUsingS3Select = async (params) => {
    // 1
    return new Promise((resolve, reject) => {
        s3.selectObjectContent(params, (err, data) => {
            if (err) { reject(err); }

            if (!data) {
                reject('Empty data object');
            }

            // This will be an array of bytes of data, to be converted
            // to a buffer
            const records = []

            // This is a stream of events
            data.Payload.on('data', (event) => {
                // There are multiple events in the eventStream, but all we 
                // care about are Records events. If the event is a Records 
                // event, there is data inside it
                if (event.Records) {
                    records.push(event.Records.Payload);
                }
            })
                .on('error', (err) => {
                    reject(err);
                })
                .on('end', () => {
                    // Convert the array of bytes into a buffer, and then
                    // convert that to a string
                    let planetString = Buffer.concat(records).toString('utf8');

                    // 2
                    // remove any trailing commas
                    planetString = planetString.replace(/\,$/, '');

                    // 3
                    // Add into JSON 'array'
                    planetString = `[${planetString}]`;

                    try {
                        const planetData = JSON.parse(planetString);
                        resolve(planetData);
                    } catch (e) {
                        reject(new Error(`Unable to convert S3 data to JSON object. S3 Select Query: ${params.Expression}`));
                    }
                });
        });
    })
}












































// end



// setTimeout(() => {
//     getData()
// }, 2000);

// CORS is enabled for all origins
app.use(cors());

app.get('/', (req, res) => {
    res.send('Hello World!')
})


let filesList = [];

let latestKey = null;

let finalData = null;


app.get('/get/weather/data', async (req, res) => {

    try {

        //configuring parameters
        const params = {
            Bucket: 'surya-source-s3',
            Prefix: "wheather_",
            'MaxKeys': 5000,
            // KeyCount: 2
            // // Body: fs.createReadStream(filePath),
            // Key: "folder/" + Date.now() + "_" + path.basename(filePath)
        };


        console.log(" in get files list function");

        const data = await s3.listObjects(params).promise();
        console.log(data, " datatta in bucket");

        await data.Contents.sort(function (a, b) {
            return (b.LastModified > a.LastModified) ? 1 :
                ((a.LastModified > b.LastModified) ? -1 : 0);
        });

        for (var file of data.Contents) {
            if (file.Key.endsWith('.json')) {
                //extractData(file.Key);
                console.log(file.Key, " file");
                filesList = [file];
                latestKey = file.Key;
                break;

            }
        }

        const params2 = {
            Bucket: 'surya-source-s3',
            Key: latestKey
        };
        let dataa = await s3.getObject(params2).promise();
        dataa = JSON.parse((dataa.Body).toString());

        finalData = dataa;

        if (finalData) {
            res.json({
                "success": true,
                message: "Fetching the latest data",
                finalData
            })
        } else {
            res.json({
                "success": false,
                message: "Unable to get the data"
            })
        }



    } catch (err) {
        console.log(err, " error in catch api");
        resetData()
        res.json({
            "success": false,
            message: "Unable to get the data"
        })
    }

})

app.get('/get/weather/data/old', async (req, res) => {

    try {
        // get files list
        setTimeout(async () => {
            resetData()
            await getFilesList()
        }, 0);

        // get file data
        setTimeout(async () => {
            console.log();

            if (filesList.length > 0) {
                await getFileData()
            } else {
                resetData()
            }

        }, 500);
        setTimeout(() => {
            console.log("FInal data in api", finalData);

            if (finalData) {
                res.json({
                    "success": true,
                    message: "Fetching the latest data",
                    finalData
                })
            } else {
                res.json({
                    "success": false,
                    message: "Unable to get the data"
                })
            }
            // res.json({
            //     "grs_data": [
            //         {
            //             "Date": "24_11_22",
            //             "Time": "13:34:36",
            //             "Temperature": [
            //                 28.28,
            //                 "C"
            //             ],
            //             "Pressure": [
            //                 1007.5,
            //                 "hpa"
            //             ],
            //             "Humidity": [
            //                 76.79,
            //                 "%RH"
            //             ],
            //             "GRS-id": 1
            //         }
            //     ]
            // }) 
        }, 1000);

    } catch (err) {
        console.log(err, " error in catch api");
        resetData()
        res.json({
            "success": false,
            message: "Unable to get the data"
        })
    }

})

// app.listen(port, () => {
//     console.log(`Example app listening on port ${port}`)
// })
app.listen(port, process.env.HOST || "0.0.0.0", () => {
    console.log(`Example app listening on port ${port}, host on ${JSON.stringify(app.listen().address())}`)
})

setTimeout(() => {

    configure()
}, 1000);
