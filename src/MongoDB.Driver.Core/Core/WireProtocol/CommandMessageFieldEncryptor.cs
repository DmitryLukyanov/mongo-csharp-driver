﻿/* Copyright 2019-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.WireProtocol.Messages;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders.BinaryEncoders;

namespace MongoDB.Driver.Core.WireProtocol
{
    internal class CommandMessageFieldEncryptor
    {
        // private fields
        private readonly byte[] _buffer = new byte[1024];
        private readonly IBinaryDocumentFieldEncryptor _documentFieldEncryptor;
        private readonly MessageEncoderSettings _messageEncoderSettings;

        // constructors
        public CommandMessageFieldEncryptor(IBinaryDocumentFieldEncryptor documentFieldEncryptor, MessageEncoderSettings messageEncoderSettings)
        {
            _documentFieldEncryptor = documentFieldEncryptor;
            _messageEncoderSettings = messageEncoderSettings;
        }

        // public static methods
        public CommandRequestMessage EncryptFields(string databaseName, CommandRequestMessage unencryptedRequestMessage, CancellationToken cancellationToken)
        {
            var unencryptedDocumentBytes = GetUnencryptedDocumentBytes(unencryptedRequestMessage);
            var encryptedDocumentBytes = _documentFieldEncryptor.EncryptFields(databaseName, unencryptedDocumentBytes, cancellationToken);
            return CreateEncryptedRequestMessage(unencryptedRequestMessage, encryptedDocumentBytes, databaseName);
        }

        public async Task<CommandRequestMessage> EncryptFieldsAsync(string databaseName, CommandRequestMessage unencryptedRequestMessage, CancellationToken cancellationToken)
        {
            var unencryptedDocumentBytes = GetUnencryptedDocumentBytes(unencryptedRequestMessage);
            var encryptedDocumentBytes = await _documentFieldEncryptor.EncryptFieldsAsync(databaseName, unencryptedDocumentBytes, cancellationToken).ConfigureAwait(false);
            return CreateEncryptedRequestMessage(unencryptedRequestMessage, encryptedDocumentBytes, databaseName);
        }

        // private static methods
        private byte[] CombineCommandMessageSectionsIntoSingleDocument(Stream stream)
        {
            using (var inputStream = new BsonStreamAdapter(stream, ownsStream: false))
            using (var memoryStream = new MemoryStream())
            using (var outputStream = new BsonStreamAdapter(memoryStream, ownsStream: false))
            {
                var messageStartPosition = inputStream.Position;
                var messageLength = inputStream.ReadInt32();
                var messageEndPosition = messageStartPosition + messageLength;
                var requestId = inputStream.ReadInt32();
                var responseTo = inputStream.ReadInt32();
                var opcode = inputStream.ReadInt32();
                var flags = (OpMsgFlags)inputStream.ReadInt32();
                if (flags.HasFlag(OpMsgFlags.ChecksumPresent))
                {
                    messageEndPosition -= 4; // ignore checksum
                }

                CopyType0Section(inputStream, outputStream);
                outputStream.Position -= 1;
                while (inputStream.Position < messageEndPosition)
                {
                    CopyType1Section(inputStream, outputStream);
                }
                outputStream.WriteByte(0);
                outputStream.BackpatchSize(0);

                return memoryStream.ToArray();
            }
        }

        private void CopyBsonDocument(BsonStream inputStream, BsonStream outputStream)
        {
            var documentLength = inputStream.ReadInt32();
            inputStream.Position -= 4;
            CopyBytes(inputStream, outputStream, documentLength);
        }

        private void CopyBytes(BsonStream inputStream, BsonStream outputStream, int count)
        {
            while (count > 0)
            {
                var chunkSize = Math.Min(count, _buffer.Length);
                inputStream.ReadBytes(_buffer, 0, chunkSize);
                outputStream.WriteBytes(_buffer, 0, chunkSize);
                count -= chunkSize;
            }
        }

        private void CopyType0Section(BsonStream inputStream, BsonStream outputStream)
        {
            var payloadType = (PayloadType)inputStream.ReadByte();
            if (payloadType != PayloadType.Type0)
            {
                throw new FormatException("Expected first section to be of type 0.");
            }

            CopyBsonDocument(inputStream, outputStream);
        }

        private void CopyType1Section(BsonStream inputStream, BsonStream outputStream)
        {
            var payloadType = (PayloadType)inputStream.ReadByte();
            if (payloadType != PayloadType.Type1)
            {
                throw new FormatException("Expected subsequent sections to be of type 1.");
            }

            var sectionStartPosition = inputStream.Position;
            var sectionSize = inputStream.ReadInt32();
            var sectionEndPosition = sectionStartPosition + sectionSize;
            var identifier = inputStream.ReadCString(Utf8Encodings.Lenient);

            outputStream.WriteByte((byte)BsonType.Array);
            outputStream.WriteCString(identifier);
            var arrayStartPosition = outputStream.Position;
            outputStream.WriteInt32(0); // array length will be backpatched
            var index = 0;
            while (inputStream.Position < sectionEndPosition)
            {
                outputStream.WriteByte((byte)BsonType.Document);
                outputStream.WriteCString(index.ToString());
                CopyBsonDocument(inputStream, outputStream);
            }
            outputStream.WriteByte(0);
            outputStream.BackpatchSize(arrayStartPosition);
        }

        private CommandRequestMessage CreateEncryptedRequestMessage(CommandRequestMessage unencryptedRequestMessage, byte[] encryptedDocumentBytes, string database)
        {
            //var serializer = (unencryptedRequestMessage.WrappedMessage.Sections[0] as Type0CommandMessageSection).DocumentSerializer as ElementAppendingSerializer<RawBsonDocument>;

            //var encryptedDocument = new RawBsonDocument(encryptedDocumentBytes);
            ////var list = encryptedSections.ToList();

            //var lis1t = new List<BsonElement>();
            //lis1t.Add(new BsonElement("$db", "default"));
            //Action<BsonWriterSettings> writerSettingsConfigurator = s => s.GuidRepresentation = GuidRepresentation.Unspecified;
            //var elementAppendingSerializer = new ElementAppendingSerializer<RawBsonDocument>(RawBsonDocumentSerializer.Instance, lis1t, writerSettingsConfigurator);
            //var encryptedSections = new[] { new Type0CommandMessageSection<RawBsonDocument>(encryptedDocument, elementAppendingSerializer) };


            //var unencryptedCommandMessage = unencryptedRequestMessage.WrappedMessage;
            //var encryptedCommandMessage = new CommandMessage(
            //    unencryptedCommandMessage.RequestId,
            //    unencryptedCommandMessage.ResponseTo,
            //    encryptedSections,
            //    unencryptedCommandMessage.MoreToCome);
            //return new CommandRequestMessage(encryptedCommandMessage, unencryptedRequestMessage.ShouldBeSent);
            {
                var str = new RawBsonDocument(encryptedDocumentBytes).ToString();
                if (str.ToLower().Contains("listcollections"))
                {
                    //todo: do nothing
                }
                //var encryptedDocument = new RawBsonDocument(encryptedDocumentBytes);
                // todo: workaround, probably it's better to update serializer
                //var doc = encryptedDocument.ToString();
                var encryptedDocument = BsonDocument.Parse(str);
                // todo: workaround, probably it's better to update serializer

                var extraElements = new List<BsonElement>();

                if (!string.IsNullOrWhiteSpace(database) && !str.Contains("$db"))
                {
                    var dbElement = new BsonElement("$db", database);
                    extraElements.Add(dbElement);
                }
                Action<BsonWriterSettings> writerSettingsConfigurator = s => s.GuidRepresentation = GuidRepresentation.Unspecified;
                var elementAppendingSerializer = new ElementAppendingSerializer<BsonDocument>(BsonDocumentSerializer.Instance, extraElements, writerSettingsConfigurator);


                var encryptedSections = new[] { new Type0CommandMessageSection<BsonDocument>(encryptedDocument, elementAppendingSerializer) };
                var unencryptedCommandMessage = unencryptedRequestMessage.WrappedMessage;
                var encryptedCommandMessage = new CommandMessage(
                    unencryptedCommandMessage.RequestId,
                    unencryptedCommandMessage.ResponseTo,
                    encryptedSections,
                    unencryptedCommandMessage.MoreToCome);
                return new CommandRequestMessage(encryptedCommandMessage, unencryptedRequestMessage.ShouldBeSent);
            }
        }

        private byte[] GetUnencryptedDocumentBytes(CommandRequestMessage unencryptedRequestMessage)
        {
            using (var stream = new MemoryStream())
            {
                WriteUnencryptedRequestMessageToStream(stream, unencryptedRequestMessage);
                stream.Position = 0;
                return CombineCommandMessageSectionsIntoSingleDocument(stream);
            }
        }

        private void WriteUnencryptedRequestMessageToStream(
            Stream stream, 
            CommandRequestMessage unencryptedRequestMessage)
        {
            var clonedMessageEncoderSettings = _messageEncoderSettings.Clone();
            clonedMessageEncoderSettings.Set(MessageEncoderSettingsName.MaxDocumentSize, 2097152);
            clonedMessageEncoderSettings.Set(MessageEncoderSettingsName.MaxMessageSize, 6000000);
            var encoderFactory = new BinaryMessageEncoderFactory(stream, clonedMessageEncoderSettings, compressorSource: null);
            var encoder = encoderFactory.GetCommandRequestMessageEncoder();
            encoder.WriteMessage(unencryptedRequestMessage);
        }
    }
}
