const express = require('express')

const cors = require('cors');


const app = express()
const port = 8787

// CORS is enabled for all origins
app.use(cors());

app.get('/', (req, res) => {
    res.send('Hello World!')
})


app.get('/get/weather/data', (req, res) => {
    res.json({
        "grs_data": [
            {
                "Date": "24_11_22",
                "Time": "13:34:36",
                "Temperature": [
                    28.28,
                    "C"
                ],
                "Pressure": [
                    1007.5,
                    "hpa"
                ],
                "Humidity": [
                    76.79,
                    "%RH"
                ],
                "GRS-id": 1
            }
        ]
    })
})

app.listen(port, "0.0.0.0", () => {
    console.log(`Example app listening on port ${port}`)
})
