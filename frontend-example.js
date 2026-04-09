/* ═══════════════════════════════════════════════════
   Add this to tgp-dashboard.html script block
   Replace PROXY_URL with your Render URL
═══════════════════════════════════════════════════ */

var PROXY_URL = 'https://your-recharge-proxy.onrender.com';

function getCustomerEmail() {
  try {
    var c = JSON.parse(sessionStorage.getItem('loginCustomer') || 'null');
    return c && c.email ? c.email : null;
  } catch(e) { return null; }
}

function loadRechargeSubscriptions() {
  var email = getCustomerEmail();
  if (!email) {
    document.getElementById('recharge-content').innerHTML =
      '<p style="color:#aaa;padding:20px">Unable to identify customer session.</p>';
    return;
  }

  document.getElementById('recharge-content').innerHTML =
    '<p style="color:#aaa;padding:20px;text-align:center"><i class="fas fa-spinner fa-spin"></i> Loading Recharge subscriptions...</p>';

  fetch(PROXY_URL + '/recharge/subscriptions?email=' + encodeURIComponent(email))
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (!data.subscriptions || !data.subscriptions.length) {
        document.getElementById('recharge-content').innerHTML =
          '<p style="color:#aaa;padding:20px">No Recharge subscriptions found.</p>';
        return;
      }
      renderRechargeTable(data.subscriptions);
    })
    .catch(function() {
      document.getElementById('recharge-content').innerHTML =
        '<p style="color:#f87171;padding:20px">Error loading subscriptions. Please try again.</p>';
    });
}

function renderRechargeTable(subs) {
  var html = '<div class="row text-center d-none d-lg-flex cc-purchase-table-cell-header">';
  html += '<div class="col table-header cc-purchase-table-header-title">Product</div>';
  html += '<div class="col table-header cc-purchase-table-header-title">Status</div>';
  html += '<div class="col table-header cc-purchase-table-header-title">Price</div>';
  html += '<div class="col table-header cc-purchase-table-header-title">Frequency</div>';
  html += '<div class="col table-header cc-purchase-table-header-title">Next Charge</div>';
  html += '<div class="col table-header cc-purchase-table-header-title">Actions</div>';
  html += '</div>';

  subs.forEach(function(s) {
    var statusColor = s.status === 'ACTIVE'    ? '#4ADE80' :
                      s.status === 'CANCELLED' ? '#F87171' : '#F59E0B';
    var nextDate = s.nextChargeDate
      ? new Date(s.nextChargeDate).toLocaleDateString('en-US', {month:'short', day:'numeric', year:'numeric'})
      : '--';
    var freq = 'Every ' + s.intervalFrequency + ' ' + (s.intervalUnit || 'month');

    html += '<div class="row text-center cc-purchase-table-row" data-sub-id="' + s.id + '">';
    html += '<div class="col cc-purchase-table-cell-value">';
    html += '<strong>' + (s.productTitle || '--') + '</strong>';
    if (s.variantTitle) html += '<br><small style="color:#aaa">' + s.variantTitle + '</small>';
    html += '</div>';
    html += '<div class="col cc-purchase-table-cell-value"><span style="color:' + statusColor + ';font-weight:600">' + s.status + '</span></div>';
    html += '<div class="col cc-purchase-table-cell-value">$' + parseFloat(s.price).toFixed(2) + '</div>';
    html += '<div class="col cc-purchase-table-cell-value">' + freq + '</div>';
    html += '<div class="col cc-purchase-table-cell-value">' + nextDate + '</div>';
    html += '<div class="col cc-purchase-table-cell-value">';

    if (s.status === 'ACTIVE') {
      html += '<a class="cc-purchase-table-action-btn bg-warning text-white rounded m-1 d-inline-block" onclick="rcPause(' + s.id + ')">Pause 3mo</a> ';
      html += '<a class="cc-purchase-table-action-btn bg-danger  text-white rounded m-1 d-inline-block" onclick="rcCancel(' + s.id + ')">Cancel</a>';
    } else if (s.status === 'CANCELLED') {
      html += '<a class="cc-purchase-table-action-btn bg-success text-white rounded m-1 d-inline-block" onclick="rcActivate(' + s.id + ')">Reactivate</a>';
    }
    html += '</div></div>';
  });

  document.getElementById('recharge-content').innerHTML = html;
}

/* Action functions */
function rcCancel(subId) {
  if (!confirm('Cancel this Recharge subscription?')) return;
  fetch(PROXY_URL + '/recharge/subscriptions/' + subId + '/cancel', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reason: 'Customer requested cancellation' })
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (d.success) loadRechargeSubscriptions();
    else alert('Error: ' + (d.error || 'Unknown error'));
  });
}

function rcPause(subId) {
  fetch(PROXY_URL + '/recharge/subscriptions/' + subId + '/pause', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ months: 3 })
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (d.success) {
      alert('Subscription paused until ' + d.nextChargeDate);
      loadRechargeSubscriptions();
    } else alert('Error: ' + (d.error || 'Unknown error'));
  });
}

function rcActivate(subId) {
  fetch(PROXY_URL + '/recharge/subscriptions/' + subId + '/activate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({})
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (d.success) loadRechargeSubscriptions();
    else alert('Error: ' + (d.error || 'Unknown error'));
  });
}
