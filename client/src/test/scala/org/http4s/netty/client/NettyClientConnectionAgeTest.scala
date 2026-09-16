/*
 * Copyright 2020 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.netty.client

import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Deferred
import cats.syntax.all._
import com.comcast.ip4s._
import munit.catseffect.IOFixture
import org.http4s.HttpRoutes
import org.http4s.Request
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import org.http4s.server.Server

import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import scala.concurrent.duration._

class NettyClientConnectionAgeTest extends IOSuite {

  // Client registered before server so it tears down first (reverse order),
  // ensuring pooled connections are closed before the server shuts down.
  val shortAgeClient: IOFixture[Client[IO]] =
    resourceFixture(
      NettyClientBuilder[IO]
        .withMaxConnectionAge(2.seconds)
        .resource,
      "short-age-client")

  val server: IOFixture[Server] = resourceFixture(
    EmberServerBuilder
      .default[IO]
      .withPort(port"0")
      .withHttpApp(
        HttpRoutes
          .of[IO] {
            case req @ GET -> Root / "port" =>
              Ok(req.remote.fold("unknown")(_.port.value.toString))
            case GET -> Root / "slow" => IO.sleep(2.seconds) >> Ok("slow")
          }
          .orNotFound
      )
      .build,
    "server"
  )

  test("connection is replaced after max age expires") {
    val s = server()
    val c = shortAgeClient()

    val req = Request[IO](uri = s.baseUri / "port")
    for {
      port1 <- c.expect[String](req)
      _ <- IO.sleep(3.seconds)
      port2 <- c.expect[String](req)
    } yield assertNotEquals(port1, port2, "expected a new connection (different remote port)")
  }

  test("in-flight request completes even if connection age expires during request") {
    val s = server()
    val c = shortAgeClient()

    val req = Request[IO](uri = s.baseUri / "slow")
    for {
      r <- c.expect[String](req)
    } yield assertEquals(r, "slow")
  }

  test("idle connection in pool is proactively closed after max age expires") {
    val httpResponse =
      "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"
    // Use a raw TCP server so we can observe when the client closes the connection
    // without triggering any pool acquire/release.
    val serverSocketR =
      Resource.make(IO.blocking(new ServerSocket(0)))(ss => IO.blocking(ss.close()))
    val clientR = NettyClientBuilder[IO].withMaxConnectionAge(2.seconds).resource

    (serverSocketR, clientR, Deferred[IO, Unit].toResource).tupled.use {
      case (serverSocket, client, clientClosed) =>
        val port = serverSocket.getLocalPort
        // Accept one connection and serve a minimal HTTP response, then watch for close
        Resource
          .make(IO.blocking(serverSocket.accept()))(socket => IO.blocking(socket.close()))
          .use { socket =>
            val in = socket.getInputStream
            val out = socket.getOutputStream
            // Read the HTTP request (consume until empty line)

            for {
              _ <- IO.blocking(in.read(new Array[Byte](4096)))
              // Send a minimal HTTP/1.1 response
              _ <- IO.blocking(out.write(httpResponse.getBytes(StandardCharsets.US_ASCII)))
              _ <- IO.blocking(out.flush())
              // Now wait for the client to close the connection
              // read() returns -1 when the peer closes
              _ <- IO.blocking(in.read()).iterateUntil(_ == -1)
              // only goes here if in.read returned -1
              _ <- clientClosed.complete(())
            } yield ()
          }
          .background
          .use { _ =>
            for {
              _ <- client.expect[String](
                Request[IO](uri = Uri.unsafeFromString(s"http://localhost:$port/test"))
              )
              // Connection is idle in the pool. Wait for max age (2s) + buffer.
              // If proactive eviction works, the client closes the connection during
              // this sleep — without any acquire to trigger the health check.
              result <- clientClosed.get
                .as(true)
                .timeoutTo(5.seconds, IO.pure(false))
            } yield assert(
              result,
              "expected idle connection to be closed proactively after max age")

          }
    }
  }
}
