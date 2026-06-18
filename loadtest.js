import http from 'k6/http';
import { check } from 'k6';

// Menggunakan library K6 untuk generate UUID acak
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
    scenarios: {
        stress_test: {
            executor: 'shared-iterations',
            vus: 50,             // 50 Virtual Users (Thread) yang menembak bersamaan
            iterations: 20000,   // Total 20.000 request
            maxDuration: '1m',   // Maksimal waktu eksekusi 1 menit
        },
    },
};

export default function () {
    // Menembak langsung ke container aggregator di port 8080 dalam jaringan internal
    const url = 'http://aggregator:8080/publish';
    
    const payload = JSON.stringify({
        topic: 'k6_stress_test',
        event_id: uuidv4(),
        timestamp: new Date().toISOString(),
        source: 'k6_load_test',
        payload: { test_value: Math.floor(Math.random() * 100) }
    });

    const params = {
        headers: { 'Content-Type': 'application/json' },
    };

    const res = http.post(url, payload, params);
    
    check(res, {
        'status was 200': (r) => r.status === 200,
    });
}
