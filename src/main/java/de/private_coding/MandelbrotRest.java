package de.private_coding;
/**
 * Created by Bartz, Tobias @Tobi-PC on 08.10.2015 at 10:47.
 */

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.net.httpserver.HttpServer;

import javax.imageio.ImageIO;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Path("/Mandelbrot")
public class MandelbrotRest {

    private static int number = 0;

    @GET
    @Path("/getMandelbrot")
    @Produces("image/png")
    public BufferedImage getMandelbrot(@QueryParam("w") int width, @QueryParam("h") int height, @QueryParam("it") int iterations) {

        return generateMandelbrot(width, height, iterations);
    }

    @GET
    @Path("/generateToS3")
    public Response generateToS3(@QueryParam("bucketname") String bucketname) {
        String keyName = String.format("image%s.png", number);
        number++;
        String uploadFileName = "image.png";

        File file = new File(uploadFileName);
        try {
            ImageIO.write(generateMandelbrot(500,500,10), "png", file);
        } catch (IOException e) {
            return Response.serverError().build();
        }

        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        if (!s3Client.doesBucketExist(bucketname)) {
            s3Client.createBucket(bucketname);

        }

        List<PartETag> partETags = new ArrayList<PartETag>();
        AccessControlList acl = new AccessControlList();
        acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("image/png");
        InitiateMultipartUploadRequest initRequest = new
                InitiateMultipartUploadRequest(bucketname, keyName)
                .withObjectMetadata(metadata)
                .withAccessControlList(acl);
        InitiateMultipartUploadResult initResponse =
                s3Client.initiateMultipartUpload(initRequest);

        long contentLength = file.length();
        long partSize = 5242880;

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 5 MB. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketname).withKey(keyName)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // Upload part and add response to our list.
                partETags.add(
                        s3Client.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(
                    bucketname,
                    keyName,
                    initResponse.getUploadId(),
                    partETags);

            s3Client.completeMultipartUpload(compRequest);
        } catch (Exception e) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                    bucketname, keyName, initResponse.getUploadId()));
        }

        try {
            return Response.seeOther(new URI(String.format("https://s3.amazonaws.com/%s/%s", bucketname, keyName))).build();
        } catch (URISyntaxException e) {
            return Response.serverError().build();
        }

    }

    @GET
    @Path("/deleteFromS3")
    public Response deleteFromS3(@QueryParam("bucketname") String bucketname) {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());

        try {
            ObjectListing objectListing = s3client.listObjects(bucketname);

            while (true) {
                for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                    S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
                    s3client.deleteObject(bucketname, objectSummary.getKey());
                }

                if (objectListing.isTruncated()) {
                    objectListing = s3client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            VersionListing list = s3client.listVersions(new ListVersionsRequest().withBucketName(bucketname));
            for (Iterator<?> iterator = list.getVersionSummaries().iterator(); iterator.hasNext(); ) {
                S3VersionSummary s = (S3VersionSummary) iterator.next();
                s3client.deleteVersion(bucketname, s.getKey(), s.getVersionId());
            }
            s3client.deleteBucket(bucketname);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        }
        return Response.ok("Bucket geloescht").build();
    }

    private BufferedImage generateMandelbrot(int width, int height, int iterations) {
        int max = iterations;
        BufferedImage image = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_RGB);
        int black = 0;
        int[] colors = new int[max];
        for (int i = 0; i < max; i++) {
            colors[i] = Color.HSBtoRGB(i / 256f, 1, i / (i + 8f));
        }
        for (int row = 0; row < height; row++) {
            for (int col = 0; col < width; col++) {
                double c_re = (col - width / 2) * 4.0 / width;
                double c_im = (row - height / 2) * 4.0 / width;
                double x = 0, y = 0;
                int iteration = 0;
                while (x * x + y * y < 4 && iteration < max) {
                    double x_new = x * x - y * y + c_re;
                    y = 2 * x * y + c_im;
                    x = x_new;
                    iteration++;
                }
                if (iteration < max) image.setRGB(col, row, colors[iteration]);
                else image.setRGB(col, row, black);
            }
        }
        return image;
    }


    public static void main(String[] args) {
        try {
            HttpServer server = HttpServerFactory.create("http://0.0.0.0:8080/");
            server.start();
            // For windows standalone implementation without cli
            //JOptionPane.showMessageDialog(null, "Stop server!");
            //server.stop(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

//TODO: Bucket Geschwindigkeit: Amazon -> 4.6 ms, total -> 69.4 ms; CloudFront: Amazon -> 4.6 ms, total: 67.4