(() => {
  "use strict";

  const form = document.getElementById("test-form");
  const field = (id) => document.getElementById(id);
  const target = field("target");
  const tls = field("tls");
  const verify = field("verify");
  const action = field("action");
  const result = field("result");
  const button = field("run");

  function isCustom() {
    const option = target.selectedOptions[0];
    return !option || option.value === form.dataset.custom;
  }

  // Presets carry their own TLS settings, so the checkboxes only show them.
  function syncFields() {
    const custom = isCustom();
    field("custom-fields").hidden = !custom;
    tls.disabled = !custom;
    if (!custom) {
      const option = target.selectedOptions[0];
      tls.checked = option.dataset.tls === "true";
      verify.checked = option.dataset.verify === "true";
    }
    verify.disabled = !custom || !tls.checked;
    field("topic-field").hidden = action.value === "check";
    field("message-field").hidden = action.value !== "produce";
  }

  // Built with textContent only: consumed messages and broker errors are data, and
  // must never be parsed as markup.
  function show(kind, title, details) {
    const box = document.createElement("div");
    box.className = `result ${kind}`;
    const heading = document.createElement("h2");
    heading.textContent = title;
    box.append(heading);
    if (details !== undefined) {
      const pre = document.createElement("pre");
      pre.textContent = typeof details === "string" ? details : JSON.stringify(details, null, 2);
      box.append(pre);
    }
    result.replaceChildren(box);
  }

  async function run(event) {
    event.preventDefault();
    const payload = {
      target: target.value,
      action: action.value,
      topic: field("topic").value.trim(),
      message: field("message").value,
      bootstrap: field("bootstrap").value.trim(),
      tls: tls.checked,
      verify: verify.checked,
    };
    // A produce can spend the server's timeout three times over: on bootstrap, on
    // creating the topic, and on the send itself.
    const waitMs = Number(form.dataset.timeoutMs) * 3 + 5000;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), waitMs);
    button.disabled = true;
    show("pending", "Testing...");
    try {
      const response = await fetch(form.dataset.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      const body = await response.json();
      if (body.success) {
        show("success", `${body.action} succeeded in ${body.elapsed_ms} ms`, body);
      } else {
        const lines = [body.error, ...(body.client_log || []).map((line) => `client: ${line}`)];
        show("error", `Failed (HTTP ${response.status})`, lines.join("\n"));
      }
    } catch (error) {
      const message = error.name === "AbortError"
        ? `No answer after ${waitMs / 1000} seconds. The broker may be unreachable, or advertising addresses this server cannot reach.`
        : String(error);
      show("error", "Failed", message);
    } finally {
      clearTimeout(timer);
      button.disabled = false;
    }
  }

  target.addEventListener("change", syncFields);
  tls.addEventListener("change", syncFields);
  action.addEventListener("change", syncFields);
  form.addEventListener("submit", run);
  syncFields();
})();
