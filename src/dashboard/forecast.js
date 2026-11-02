// The forecast of a place at a glance, a Streamlit component (st.components.v2, forecast.py): its next hours and its
// next days, as a weather app shows them, each with its icon, its temperatures and its precipitation. A day is a
// button: clicked, it sends its date to Python, which makes it the day of the page. Each run draws the rows again,
// where they were scrolled to.

function element(tag, className, text) {
  const made = document.createElement(tag);
  if (className) made.className = className;
  if (text) made.textContent = text;
  return made;
}

// The icon of a weather code, or an empty square of its size without one: the columns stay aligned
function icon(address, alt) {
  if (!address) return element("div", "icon");
  const image = element("img", "icon");
  Object.assign(image, { src: address, alt, title: alt });
  return image;
}

function rain(text, drop) {
  const line = element("div", "rain");
  if (text) {
    const image = element("img", "drop");
    image.src = drop;
    image.alt = "";
    line.append(image, text);
  }
  return line;
}

function hour(item, drop) {
  const cell = element("div", "hour");
  cell.append(
    element("div", "over", item.over),
    element("div", "label", item.label),
    icon(item.icon, item.sky),
    element("div", "high", item.temperature),
    rain(item.rain, drop),
  );
  return cell;
}

// A day, its temperatures on a bar spanning its low to its high on the scale of all the days of the row
function day(item, drop, setTriggerValue) {
  const button = element("button", item.selected ? "day selected" : "day");
  button.type = "button";
  button.title = item.title;
  const track = element("div", "track");
  const bar = element("div", "bar");
  bar.style.top = `${item.top * 100}%`;
  bar.style.height = `${Math.max(item.height * 100, 8)}%`;
  track.append(bar);
  button.append(
    element("div", "name", item.name),
    element("div", "muted", item.date_label),
    icon(item.icon, item.sky),
    element("div", "high", item.high),
    track,
    element("div", "muted", item.low),
    rain(item.rain, drop),
  );
  button.addEventListener("click", () => setTriggerValue("day", item.date));
  return button;
}

export default function (component) {
  const { data, parentElement, setTriggerValue } = component;
  let root = parentElement.querySelector(".forecast");
  if (!root) {
    root = element("div", "forecast");
    parentElement.append(root);
  }
  const scrolled = [...root.querySelectorAll(".row")].map((row) => row.scrollLeft);
  root.classList.toggle("dark", data.dark);
  root.style.setProperty("--accent", data.accent);
  const hours = element("div", "row");
  hours.append(...data.hours.map((item) => hour(item, data.drop)));
  const days = element("div", "row");
  days.append(...data.days.map((item) => day(item, data.drop, setTriggerValue)));
  root.replaceChildren(
    element("div", "heading", data.hours_title),
    hours,
    element("div", "heading", data.days_title),
    days,
  );
  root.querySelectorAll(".row").forEach((row, index) => {
    row.scrollLeft = scrolled[index] ?? 0;
  });
}
