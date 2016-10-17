package br.com.smartmei.importer.worker;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.DatabaseReference.CompletionListener;

public class ChunkSaver implements Runnable {
	
	private CountDownLatch latch;
	private DatabaseReference ref;
	private Map<String, Object> data;
	
	public ChunkSaver(CountDownLatch latch, DatabaseReference ref, Map<String, Object> data) {
	    this.latch = latch;
	    this.ref = ref;
	    this.data = data;
	}

	@Override
	public void run() {
		this.save();
	}

	private void save() {
		this.ref.updateChildren(data, new CompletionListener() {
			@Override
			public void onComplete(DatabaseError error, DatabaseReference ref) {
				if (error != null) {
		            System.err.println("Data could not be saved " + error.getMessage());
		        }
				latch.countDown();				
			}
		});
	}

}
