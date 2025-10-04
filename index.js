document.addEventListener('DOMContentLoaded', function() {
  const timeWindow = document.getElementById('time_window');
  const customDate = document.getElementById('custom_date');

  timeWindow.addEventListener('change', function() {
    if (this.value === 'custom') {
      customDate.style.display = 'inline';
    } else {
      customDate.style.display = 'none';
    }
  });
});