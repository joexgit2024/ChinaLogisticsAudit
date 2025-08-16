/**
 * Audit Actions JavaScript
 * 
 * This file handles the functionality for the audit actions,
 * specifically preventing the FedEx dropdown menu from collapsing
 * when clicking the Run Batch Audit button.
 */

document.addEventListener('DOMContentLoaded', function() {
    // Find the Run Batch Audit button if it exists
    const batchAuditBtn = document.querySelector('.btn-run-batch-audit');
    
    if (batchAuditBtn) {
        // Add event listener to prevent event propagation
        batchAuditBtn.addEventListener('click', function(event) {
            // Stop event propagation to prevent it from bubbling up to dropdown controls
            event.stopPropagation();
            
            // Log that the click was captured
            console.log('Run Batch Audit button clicked, propagation stopped');
        });
    }
});
