#!/usr/bin/env python3
"""
Command-line script to run batch reprocessing of incomplete invoice extractions
"""

import argparse
import sys
from batch_reprocess_invoices import InvoiceReprocessor

def main():
    """Main entry point for batch reprocessing command-line script"""
    parser = argparse.ArgumentParser(
        description='Reprocess incomplete invoice extractions with Llama 3.2'
    )
    
    parser.add_argument(
        '--limit', 
        type=int, 
        default=None,
        help='Limit number of invoices to reprocess (default: process all)'
    )
    
    parser.add_argument(
        '--threshold', 
        type=float, 
        default=0.7,
        help='Confidence threshold for reprocessing (default: 0.7)'
    )
    
    parser.add_argument(
        '--db', 
        default='dhl_audit.db',
        help='Database path (default: dhl_audit.db)'
    )
    
    args = parser.parse_args()
    
    print(f"Starting batch reprocessing with:")
    print(f"  - Database: {args.db}")
    print(f"  - Confidence threshold: {args.threshold}")
    if args.limit:
        print(f"  - Limit: {args.limit} invoices")
    else:
        print("  - Processing all incomplete invoices")
    
    # Create reprocessor and run batch job
    reprocessor = InvoiceReprocessor(db_path=args.db)
    
    try:
        results = reprocessor.run_batch_reprocessing(
            limit=args.limit,
            confidence_threshold=args.threshold
        )
        
        # Print summary
        print("\nReprocessing Summary:")
        print(f"Total invoices processed: {results['total']}")
        print(f"Successfully reprocessed: {results['successful']}")
        print(f"Failed: {results['failed']}")
        print(f"Success rate: {results['success_rate']:.1%}")
        
        # Return success if at least some invoices were processed successfully
        return 0 if results['successful'] > 0 else 1
        
    except Exception as e:
        print(f"Error during batch reprocessing: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
