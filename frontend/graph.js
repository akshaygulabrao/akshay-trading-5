export function draw(id,data) {
  console.log(data)
  // 1. Parse the JSON
  const readings = {
    xs: data.readings.xs.map(d => new Date(d)),
    ys: data.readings.ys
  };
  const forecasts = {
    xs: data.forecasts.xs.map(d => new Date(d)),
    ys: data.forecasts.ys
  };

  // 2. Merge for scales
  const allX = [...readings.xs, ...forecasts.xs];
  const allY = [...readings.ys, ...forecasts.ys];

  // 3. Dimensions / margins
  const margin = {top: 20, right: 30, bottom: 30, left: 40};
  const width  = 960 - margin.left - margin.right;
  const height = 400 - margin.top - margin.bottom;

  // 4. Scales
  const x = d3.scaleUtc()
      .domain(d3.extent(allX))
      .range([0, width]);

  const y = d3.scaleLinear()
      .domain(d3.extent(allY))
      .nice()
      .range([height, 0]);

  // 5. Line generators
  const line = d3.line()
      .x((d, i) => x(d))
      .y((d, i) => y(readings.ys[i]));

  const lineF = d3.line()
      .x((d, i) => x(d))
      .y((d, i) => y(forecasts.ys[i]));

  // 6. SVG
  const svg = d3.select(id)
      .append("svg")
      .attr("viewBox", `0 0 ${width+margin.left+margin.right} ${height+margin.top+margin.bottom}`)
    .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

  // 7. Axes
  svg.append("g")
      .attr("class", "axis x")
      .attr("transform", `translate(0,${height})`)
      .call(d3.axisBottom(x).ticks(width/100));

  svg.append("g")
      .attr("class", "axis y")
      .call(d3.axisLeft(y));

  // 8. Paths
  svg.append("path")
      .datum(readings.xs)
      .attr("class", "line readings")
      .attr("fill", "none")
      .attr("stroke-width", 1.5)
      .attr("d", line);

  svg.append("path")
      .datum(forecasts.xs)
      .attr("class", "line forecasts")
      .attr("fill", "none")
      .attr("stroke-width", 1.5)
      .attr("d", lineF);

  // 9. Optional legend
  svg.append("text")
      .attr("x", width-10).attr("y", 10).attr("text-anchor", "end")
      .text("Readings");
  svg.append("text")
      .attr("x", width-10).attr("y", 25).attr("text-anchor", "end")
      .text("Forecasts");
}