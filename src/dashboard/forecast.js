// The weather of a place at a glance, as a weather app shows it, a Streamlit component (st.components.v2,
// forecast.py): the head of the day of the page, its parts, its hours, the next days and the details of the day. A
// day of the next ones is a button: clicked, it sends its date to Python, which makes it the day of the page. Each run
// draws the component again, its rows where they were scrolled to.

function element(tag, className, text) {
  const made = document.createElement(tag);
  if (className) made.className = className;
  if (text) made.textContent = text;
  return made;
}

// The icon of a weather code, or an empty square of its size without one: the columns stay aligned
function icon(address, alt, className = "icon") {
  if (!address) return element("div", className);
  const image = element("img", className);
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

// The day of the page: its icon, its temperature (now, or its high and low), its sky and how it feels
function hero(item) {
  const head = element("div", "hero");
  const temperature = element("div", "temperature number", item.temperature);
  if (item.low) temperature.append(element("span", "low", item.low));
  const text = element("div", "text");
  text.append(element("div", "when", item.when), element("div", "sky", item.sky));
  if (item.feels) text.append(element("div", "feels", item.feels));
  for (const line of item.lines) text.append(element("div", "line", line));
  head.append(icon(item.icon, item.alt), temperature, text);
  return head;
}

function part(item, drop) {
  const cell = element("div", "part");
  cell.title = `${item.name}, ${item.hours}: ${item.sky}`;
  cell.append(
    element("div", "name", item.name),
    icon(item.icon, item.sky),
    element("div", "high", item.high),
    element("div", "muted", item.low),
    rain(item.rain, drop),
  );
  return cell;
}

function hour(item, drop) {
  const cell = element("div", "hour");
  const feels = element("div", "feels-like");
  if (item.feels) feels.append(element("div", "", "feels"), item.feels);
  cell.append(
    element("div", "over", item.over),
    element("div", "label", item.label),
    icon(item.icon, item.sky),
    element("div", "high", item.temperature),
    feels,
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

// A card of the details: its icon and label, its value, with an arrow where the wind blows, and its lines
function card(item) {
  const box = element("div", "card");
  box.title = item.title;
  const head = element("div", "head");
  head.append(item.icon ? icon(item.icon, "") : element("span", "emoji", item.emoji), item.label);
  const value = element("div", "value number", item.value);
  if (item.arrow != null) {
    const arrow = element("span", "arrow", "↓");
    arrow.style.transform = `rotate(${item.arrow}deg)`;
    value.append(arrow);
  }
  box.append(head, value, ...item.lines.map((line) => element("div", "line", line)));
  return box;
}

function row(items) {
  const line = element("div", "row");
  line.append(...items);
  return line;
}

export default function (component) {
  const { data, parentElement, setTriggerValue } = component;
  let root = parentElement.querySelector(".forecast");
  if (!root) {
    root = element("div", "forecast");
    parentElement.append(root);
  }
  const scrolled = [...root.querySelectorAll(".row")].map((line) => line.scrollLeft);
  root.classList.toggle("dark", data.dark);
  root.style.setProperty("--accent", data.accent);
  const parts = element("div", "parts");
  parts.append(...data.parts.map((item) => part(item, data.drop)));
  const hours = row(data.hours.map((item) => hour(item, data.drop)));
  const days = row(data.days.map((item) => day(item, data.drop, setTriggerValue)));
  const cards = element("div", "cards");
  cards.append(...data.details.map(card));
  root.replaceChildren(
    ...(data.hero ? [hero(data.hero)] : []),
    ...(data.parts.length ? [parts] : []),
    ...(data.hours.length ? [element("div", "heading", data.hours_title), hours] : []),
    ...(data.days.length ? [element("div", "heading", data.days_title), days] : []),
    element("div", "heading", data.details_title),
    cards,
  );
  root.querySelectorAll(".row").forEach((line, index) => {
    line.scrollLeft = scrolled[index] ?? 0;
  });
}
