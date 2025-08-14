export function draw(id, data) {
    // Check if the element exists
    const container = document.getElementById(id);
    if (!container) {
        console.error(`Element with ID "${id}" not found`);
        return;
    }

    // Clear any existing canvas
    container.innerHTML = '<canvas></canvas>';
    const canvas = container.querySelector('canvas');
    const ctx = canvas.getContext('2d');

    // Parse timestamps to Date objects
    const parseTime = (timestamp) => new Date(timestamp);
    
    // Combine and sort all timestamps chronologically
    const allTimes = [...data.readings.xs, ...data.forecasts.xs]
        .map(parseTime)
        .sort((a, b) => a - b);

    // Create datasets with null values where data doesn't exist
    const readingsData = allTimes.map(time => {
        const index = data.readings.xs.findIndex(ts => 
            parseTime(ts).getTime() === time.getTime());
        return index !== -1 ? data.readings.ys[index] : null;
    });

    const forecastsData = allTimes.map(time => {
        const index = data.forecasts.xs.findIndex(ts => 
            parseTime(ts).getTime() === time.getTime());
        return index !== -1 ? data.forecasts.ys[index] : null;
    });

    // Create the Chart.js chart
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: allTimes,
            datasets: [
                {
                    label: 'Readings',
                    data: readingsData,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    tension: 0.1,
                    pointRadius: 3,
                    fill: true
                },
                {
                    label: 'Forecasts',
                    data: forecastsData,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255, 99, 132, 0.1)',
                    tension: 0.1,
                    pointRadius: 3,
                    borderDash: [5, 5],
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day',
                        tooltipFormat: 'MMM d, yyyy HH:mm',
                        displayFormats: {
                            day: 'MMM d'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Value'
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${context.parsed.y}`;
                        }
                    }
                },
                legend: {
                    position: 'top'
                }
            }
        }
    });
}